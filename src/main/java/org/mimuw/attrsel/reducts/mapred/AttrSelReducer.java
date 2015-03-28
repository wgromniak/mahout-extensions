package org.mimuw.attrsel.reducts.mapred;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.mimuw.attrsel.reducts.FrequencyScoreCalculator;

import java.io.DataInput;
import java.io.IOException;
import java.net.URI;
import java.util.List;


/**
 * Input - ( attribute, [it's reducts] ), output - ( attribute, it's score ).
 */


public final class AttrSelReducer extends Reducer<IntWritable, IntListWritable, IntWritable, DoubleWritable>  {


    private IntListWritable numberOfSubtablesPerAttribute;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();
        if (cacheFiles.length > 0) {
            Path filePath = new Path(cacheFiles[0].getPath());
            DataInput in = fs.open(filePath);
            numberOfSubtablesPerAttribute = IntListWritable.read(in);
        }
        else {
            throw new InterruptedException("No cache file found in hdfs cache!");
        }
    }

    @Override
    protected void reduce(IntWritable key, Iterable<IntListWritable> values, Context context)
            throws IOException, InterruptedException {

        int numberOfSubtables = numberOfSubtablesPerAttribute.get().get(key.get());
        Iterable<List<Integer>> realValues = Iterables.transform(values,
                new Function<IntListWritable, List<Integer>>() {
                    @Override
                    public List<Integer> apply(IntListWritable input) {
                        return input.get();
                    }
                });

        FrequencyScoreCalculator score = new FrequencyScoreCalculator(realValues, numberOfSubtables);
        DoubleWritable keyScore = new DoubleWritable(score.getScore());

        context.write(key, keyScore);
    }
}

