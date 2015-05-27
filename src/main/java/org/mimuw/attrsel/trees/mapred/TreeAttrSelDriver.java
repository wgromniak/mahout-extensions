package org.mimuw.attrsel.trees.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.mimuw.attrsel.common.SubtableInputFormat;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.mimuw.attrsel.trees.AbstractAttrSelTreesDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class TreeAttrSelDriver extends AbstractAttrSelTreesDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(TreeAttrSelDriver.class);

    @Override
    public int run(String[] args) throws Exception {
        setUpAttrSelOptions();
        setUpTreesOptions();

        if (parseArguments(args, false, true) == null) {
            return 1;
        }

        loadInputData();

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
        SubtableGenerator<Subtable> subtableGenerator = getSubtableGenerator();
        List<Subtable> subtables = subtableGenerator.getSubtables();
        SubtableInputFormat.setSubtables(subtables);

        if (!job.waitForCompletion(true)) {
            return 1;
        }

        SequenceFileDirIterable<IntWritable, DoubleWritable> dirIterable
                = new SequenceFileDirIterable<>(getOutputPath(), PathType.LIST, PathFilters.partFilter(), getConf());

        double[] scores = new double[fullInputTable.columnSize() - 1];

        for (Pair<IntWritable, DoubleWritable> attrScore : dirIterable) {
            scores[attrScore.getFirst().get()] = attrScore.getSecond().get();
        }

        printScoresAssessResults(scores);

        return 0;
    }

    public static void main(String... args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TreeAttrSelDriver(), args);
        LOGGER.info("MapReduce exited with value: {}", res);
    }
}
