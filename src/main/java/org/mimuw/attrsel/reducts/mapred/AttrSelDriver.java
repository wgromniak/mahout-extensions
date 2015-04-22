package org.mimuw.attrsel.reducts.mapred;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Matrix;
import org.mimuw.attrsel.common.*;
import org.mimuw.attrsel.common.api.CutoffPointCalculator;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public final class AttrSelDriver extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(AttrSelDriver.class);

    public static final MetricRegistry METRICS = new MetricRegistry();
    static {
        METRICS.register("MemoryGauge", new MemoryGauge());
        Slf4jReporter.forRegistry(METRICS)
                .outputTo(LoggerFactory.getLogger(name(AttrSelDriver.class, "Metrics")))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build()
                .start(5, TimeUnit.SECONDS);
    }

    @Override
    public int run(String[] args) throws Exception {
        // general options
        addInputOption(); // TODO: hack - input is treated as local-fs input, not hdfs
        addOutputOption();
        addOption("numSubtables", "numSub", "Number of subtables the original tables will be divided into", true);
        addOption("subtableCardinality", "subCard", "Cardinality of each of the subtables", true);
        addOption("subtableGenerator", "subGen", "Class of the subtable generator");
        addOption("seed", "seed", "Random number generator seed", "123456789");

        // reducts options
        addOption("IndiscernibilityForMissing", "indisc", "Indiscernibility for missing values");
        addOption("DiscernibilityMethod", "discMeth", "Discernibility method");
        addOption("GeneralizedDecisionTransitiveClosure", "genDec", "Generalized decision transitive closure");
        addOption("JohnsonReducts", "johnson", "Johnson reducts");

        Map<String, List<String>> parsedArgs = parseArguments(args, true, true);

        if (parsedArgs == null) {
            return 1;
        }


        // copy relevant options to Config
        if (hasOption("IndiscernibilityForMissing"))
            getConf().set("IndiscernibilityForMissing", getOption("IndiscernibilityForMissing"));
        if (hasOption("DiscernibilityMethod"))
            getConf().set("DiscernibilityMethod", getOption("DiscernibilityMethod"));
        if (hasOption("GeneralizedDecisionTransitiveClosure"))
            getConf().set("GeneralizedDecisionTransitiveClosure", getOption("GeneralizedDecisionTransitiveClosure"));
        if (hasOption("JohnsonReducts"))
            getConf().set("JohnsonReducts", getOption("JohnsonReducts"));


        Job job = Job.getInstance(getConf());

        SequenceFileOutputFormat.setOutputPath(job, getOutputPath());

        job.setJobName("mahout-extensions.attrsel.reducts");
        job.setJarByClass(AttrSelDriver.class); // jar with this class

        job.setMapperClass(AttrSelMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntListWritable.class);

        job.setReducerClass(AttrSelReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(SubtableInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // read from local fs
        Matrix inputDataTable = new CSVMatrixReader().read(Paths.get(getInputFile().getPath()));
        setSubtablesAndWriteCache(job, inputDataTable);

        if (!job.waitForCompletion(true)) {
            return 1;
        }

        SequenceFileDirIterable<IntWritable, DoubleWritable> dirIterable
                = new SequenceFileDirIterable<>(getOutputPath(), PathType.LIST, PathFilters.partFilter(), getConf());

        double[] scores = new double[inputDataTable.columnSize() - 1];

        for (Pair<IntWritable, DoubleWritable> attrScore : dirIterable) {
            scores[attrScore.getFirst().get()] = attrScore.getSecond().get();
        }

        System.out.println("Scores are (<attr>: <score>):");
        for (int i = 0; i < scores.length; i++) {
            System.out.printf("%s: %s%n", i, scores[i]);
        }

        CutoffPointCalculator cutoffCalculator = new FastCutoffPoint();
        List<Integer> selected = cutoffCalculator.calculateCutoffPoint(Arrays.asList(ArrayUtils.toObject(scores)));

        System.out.printf("Selected attrs: %s%n", selected);

        double acc = new TreeAccuracyValidator().validate(inputDataTable, selected);

        System.out.printf("Accuracy: %s%n", acc);

        return 0;
    }

    private void setSubtablesAndWriteCache(Job job, Matrix fullMatrix) throws Exception {

        @SuppressWarnings("unchecked")
        Class<SubtableGenerator<Subtable>> generatorClass =
                (Class<SubtableGenerator<Subtable>>) Class.forName(
                       getOption("subtableGenerator", MatrixFixedSizeObjectSubtableGenerator.class.getCanonicalName()));

        int numberOfSubtables = getInt("numSubtables");
        int subtableSize = getInt("subtableCardinality");
        long seed = Long.parseLong(getOption("seed"));

        SubtableGenerator<Subtable> subtableGenerator = generatorClass
                .getConstructor(Random.class, int.class, int.class, Matrix.class)
                .newInstance(RandomUtils.getRandom(seed), numberOfSubtables, subtableSize, fullMatrix);

        List<Subtable> subtables = subtableGenerator.getSubtables();
        List<Integer> numberOfSubtablesPerAttribute = subtableGenerator.getNumberOfSubtablesPerAttribute();

        SubtableInputFormat.setSubtables(subtables);

        Path path = new Path("/tmp/attrsel/numSubtables");

        try (FSDataOutputStream os = FileSystem.get(job.getConfiguration()).create(path, true)) {

            new IntListWritable(numberOfSubtablesPerAttribute).write(os);
        }

        job.addCacheFile(path.toUri());

        LOGGER.info("Saved number of subtables per attribute to cache");
    }

    public static void main(String... args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AttrSelDriver(), args);
        LOGGER.info("MapReduce exited with value: {}", res);
    }
}
