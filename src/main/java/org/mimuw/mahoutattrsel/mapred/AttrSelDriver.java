package org.mimuw.mahoutattrsel.mapred;

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
import org.mimuw.mahoutattrsel.CSVMatrixReader;
import org.mimuw.mahoutattrsel.MatrixFixedSizeObjectSubtableGenerator;
import org.mimuw.mahoutattrsel.api.Subtable;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Random;

public final class AttrSelDriver extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(AttrSelDriver.class);

    public static final String SEED = "mahout-extensions.attrsel.random.seed";
    public static final String SUBTABLE_GEN = "mahout-extensions.attrsel.subtable.generator";
    public static final String NUM_SUBTABLES = "mahout-extensions.attrsel.number.of.subtables";
    public static final String SUBTABLE_CARD = "mahout-extensions.attrsel.subtable.size";

    @Override
    public int run(String[] args) throws Exception {
        addInputOption(); // TODO: hack - input is treated as local-fs input, not hdfs
        addOutputOption();
        addOption("numSubtables", "numSub", "Number of subtables the original tables will be divided into", true);
        addOption("subtableCardinality", "subCard", "Cardinality of each of the subtables", true);
        addOption("subtableGenerator", "subGen", "Class of the subtable generator");
        addOption("seed", "seed", "Random number generator seed");
        Map<String, List<String>> parsedArgs = parseArguments(args, true, true);

        if (parsedArgs == null) {
            return 1;
        }

        Job job = Job.getInstance(getConf());

        SequenceFileOutputFormat.setOutputPath(job, getOutputPath());

        job.setJobName("mahout-extensions.attrsel");
        job.setJarByClass(AttrSelDriver.class); // jar with this class

        job.setMapperClass(AttrSelMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntListWritable.class);

        job.setReducerClass(AttrSelReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(SubtableInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.getConfiguration().setInt(NUM_SUBTABLES, Integer.parseInt(getOption("numSubtables")));
        job.getConfiguration().setInt(SUBTABLE_CARD, Integer.parseInt(getOption("subtableCardinality")));
        if (hasOption("subtableGenerator")) {
            job.getConfiguration().setClass(SUBTABLE_GEN, Class.forName(getOption(SUBTABLE_GEN)),
                    SubtableGenerator.class);
        }
        if (hasOption("seed")) {
            job.getConfiguration().setInt(SEED, Integer.parseInt(getOption(SEED)));
        }
        // TODO: add conf options for Mapper

        // read from local fs
        Matrix inputDataTable = new CSVMatrixReader().read(Paths.get(getInputFile().getPath()));
        setSubtablesAndWriteCache(job, inputDataTable);

        if (!job.waitForCompletion(true)) {
            return 1;
        }

        SequenceFileDirIterable<IntWritable, DoubleWritable> dirIterable
                = new SequenceFileDirIterable<>(getOutputPath(), PathType.LIST, PathFilters.partFilter(), getConf());

        // TODO: calculate cutoff point based on scores
        for (Pair<IntWritable, DoubleWritable> attrScore : dirIterable) {
            System.out.printf("Score for attr: %s is: %s%n", attrScore.getFirst(), attrScore.getSecond());
        }

        return 0;
    }

    private void setSubtablesAndWriteCache(Job job, Matrix fullMatrix) throws Exception {

        Configuration conf = job.getConfiguration();

        @SuppressWarnings("unchecked")
        Class<SubtableGenerator<Subtable>> generatorClass =
                (Class<SubtableGenerator<Subtable>>) conf.getClass(SUBTABLE_GEN,
                        MatrixFixedSizeObjectSubtableGenerator.class);

        int numberOfSubtables = conf.getInt(NUM_SUBTABLES, 1);
        int subtableSize = conf.getInt(SUBTABLE_CARD, 1);
        long seed = conf.getLong(SEED, 123456789L);

        SubtableGenerator<Subtable> subtableGenerator;

        subtableGenerator = generatorClass
                .getConstructor(Random.class, int.class, int.class, Matrix.class)
                .newInstance(RandomUtils.getRandom(seed), numberOfSubtables, subtableSize, fullMatrix);

        List<Subtable> subtables = subtableGenerator.getSubtables();
        List<Integer> numberOfSubtablesPerAttribute = subtableGenerator.getNumberOfSubtablesPerAttribute();

        SubtableInputFormat.setSubtables(subtables);

        Path path = new Path("attrsel/numSubtables");

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
