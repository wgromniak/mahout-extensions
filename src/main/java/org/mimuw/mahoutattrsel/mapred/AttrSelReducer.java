package org.mimuw.mahoutattrsel.mapred;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Input - ( attribute, [it's reducts] ), output - ( attribute, it's score ).
 */
public final class AttrSelReducer extends Reducer<IntWritable, IntListWritable, IntWritable, DoubleWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // TODO: load  data from cache
    }

    @Override
    protected void reduce(IntWritable key, Iterable<IntListWritable> values, Context context)
            throws IOException, InterruptedException {
        // TODO
    }
}
