package org.mimuw.attrsel.trees.mapred;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.mimuw.attrsel.common.CSVMatrixReader;
import org.mimuw.attrsel.common.MatrixFixedSizeObjectSubtableGenerator;
import org.mimuw.attrsel.common.SubtableInputFormat;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.mimuw.attrsel.reducts.FastCutoffPoint;
import org.mimuw.attrsel.reducts.api.CutoffPointCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

public final class TreeAttrSelDriver extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(TreeAttrSelDriver.class);

    @Override
    public int run(String[] args) throws Exception {
        // general options
        addInputOption(); // TODO: hack - input is treated as local-fs input, not hdfs
        addOutputOption();
        addOption("numSubtables", "numSub", "Number of subtables the original tables will be divided into", true);
        addOption("subtableCardinality", "subCard", "Cardinality of each of the subtables", true);
        addOption("subtableGenerator", "subGen", "Class of the subtable generator");
        addOption("seed", "seed", "Random number generator seed", "123456789");

        // mcfs options
        addOption("numberOfTrees", "numTrees", "Number of trees in each map task");
        addOption("u", "u", "u param from the paper");
        addOption("v", "v", "v param from the paper");

        Map<String, List<String>> parsedArgs = parseArguments(args, true, true);

        if (parsedArgs == null) {
            return 1;
        }

        // copy relevant options to Config
        if (hasOption("seed"))
            getConf().set("seed", getOption("seed"));
        if (hasOption("numberOfTrees"))
            getConf().set("numberOfTrees", getOption("numberOfTrees"));
        if (hasOption("u"))
            getConf().set("u", getOption("u"));
        if (hasOption("v"))
            getConf().set("v", getOption("v"));


        Job job = Job.getInstance(getConf());

        SequenceFileOutputFormat.setOutputPath(job, getOutputPath());

        job.setJobName("mahout-extensions.attrsel.trees");
        job.setJarByClass(TreeAttrSelDriver.class); // jar with this class

        job.setMapperClass(TreeAttrSelMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(TreeAttrSelReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(SubtableInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // read from local fs
        Matrix inputDataTable = new CSVMatrixReader().read(Paths.get(getInputFile().getPath()));
        setSubtables(inputDataTable);

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

        return 0;
    }

    private void setSubtables(Matrix fullMatrix) throws Exception {

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

        SubtableInputFormat.setSubtables(subtables);
    }

    public static void main(String... args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TreeAttrSelDriver(), args);
        LOGGER.info("MapReduce exited with value: {}", res);
    }
}
