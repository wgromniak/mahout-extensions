package org.mimuw.attrsel.reducts.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Matrix;
import org.mimuw.attrsel.common.CSVMatrixReader;
import org.mimuw.attrsel.common.SubtableInputFormat;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.mimuw.attrsel.reducts.AbstractAttrSelReductsDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.List;

public final class AttrSelDriver extends AbstractAttrSelReductsDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(AttrSelDriver.class);

    @Override
    public int run(String[] args) throws Exception {
        setUpAttrSelOptions();
        setUpReductsOptions();

        if (parseArguments(args, false, false) == null) {
            return 1;
        }

        copyOptionsToConf();

        // set-up MapRed

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

        // read results from HDFS

        SequenceFileDirIterable<IntWritable, DoubleWritable> dirIterable
                = new SequenceFileDirIterable<>(getOutputPath(), PathType.LIST, PathFilters.partFilter(), getConf());

        double[] scores = new double[inputDataTable.columnSize() - 1];

        for (Pair<IntWritable, DoubleWritable> attrScore : dirIterable) {
            scores[attrScore.getFirst().get()] = attrScore.getSecond().get();
        }

        printScoresAssessResults(scores, inputDataTable);

        return 0;
    }

    private void setSubtablesAndWriteCache(Job job, Matrix fullMatrix) throws Exception {

        SubtableGenerator<Subtable> subtableGenerator = getSubtableGenerator(fullMatrix);

        List<Subtable> subtables = subtableGenerator.getSubtables();
        List<Integer> numberOfSubtablesPerAttribute = subtableGenerator.getNumberOfSubtablesPerAttribute();

        SubtableInputFormat.setSubtables(subtables);

        Path path = new Path("/tmp/attrsel/numSubtables"); // with tmp works locally

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
