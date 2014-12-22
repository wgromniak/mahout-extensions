package org.mimuw.mahoutattrsel.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.CSVMatrixReader;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

public final class AttrSelDriver extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(AttrSelDriver.class);

    private static final String NUM_SUBTABLES = "num-subtables";
    private static final String SUBTABLE_CARD = "subtable-cardinality";
    private static final String SUBTABLE_GEN = "subtable-generator";

    @Override
    public int run(String[] args) throws Exception {
        addInputOption(); // input is treated as local-fs input, not hdfs
        addOutputOption();
        addOption(NUM_SUBTABLES, "num-sub", "Number of subtables the original tables will be divided into", true);
        addOption(SUBTABLE_CARD, "sub-card", "Cardinality of each of the subtables", true);
        addOption(SUBTABLE_GEN, "sub-gen", "Class of the subtable generator");
        parseArguments(args);

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

        job.getConfiguration().setInt(SubtableInputFormat.NUM_SUBTABLES, Integer.parseInt(getOption(NUM_SUBTABLES)));
        job.getConfiguration().setInt(SubtableInputFormat.SUBTABLE_CARD, Integer.parseInt(getOption(SUBTABLE_CARD)));
        job.getConfiguration().setClass(SubtableInputFormat.SUBTABLE_GEN, Class.forName(getOption(SUBTABLE_GEN)),
                SubtableGenerator.class);
        // TODO: add conf options for Mapper

        // read from local fs
        Matrix inputDataTable = new CSVMatrixReader().read(Paths.get(getInputFile().getPath()));

        SubtableInputFormat.setFileSystem(FileSystem.get(job.getConfiguration()));
        SubtableInputFormat.setDataTable(inputDataTable);

        if (!job.waitForCompletion(true)) {
            return 1;
        }

        SequenceFileDirIterable<IntWritable, DoubleWritable> dirIterable
                = new SequenceFileDirIterable<>(getOutputPath(), PathType.LIST, PathFilters.partFilter(), getConf());

        // TODO: calculate cutoff point based on scores
        for (Pair<IntWritable, DoubleWritable> attrScore : dirIterable) {
            System.out.printf("Score for attr: %s is: %s", attrScore.getFirst(), attrScore.getSecond());
        }

        return 0;
    }

    public static void main(String... args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AttrSelDriver(), args);
        LOGGER.info("MapReduce exited with value: {}", res);
    }
}
