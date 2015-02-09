package org.mimuw.mahoutattrsel.mapred;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import java.io.DataInput;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.mimuw.mahoutattrsel.FrequencyScoreCalculator;

import java.io.IOException;
import java.util.List;


/**
 * Input - ( attribute, [it's reducts] ), output - ( attribute, it's score ).
 */


public final class AttrSelReducer extends Reducer<IntWritable, IntListWritable, IntWritable, DoubleWritable>  {


    private IntListWritable numberOfSubtablesPerAttribute;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();
        try {
            Path filePath = new Path(cacheFiles[0].getPath());
            DataInput in = fs.open(filePath);
            numberOfSubtablesPerAttribute = IntListWritable.read(in);
        }
        catch(ArrayIndexOutOfBoundsException e){}
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

