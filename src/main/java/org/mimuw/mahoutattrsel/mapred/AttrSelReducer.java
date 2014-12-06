package org.mimuw.mahoutattrsel.mapred;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Input - ( attribute name, [it's reducts] ), output - ( attribute name, it's score ).
 */
public final class AttrSelReducer extends Reducer<Text, BitSetWritable, Text, DoubleWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // TODO: load  data from cache
    }

    @Override
    protected void reduce(Text key, Iterable<BitSetWritable> values, Context context)
            throws IOException, InterruptedException {
        // TODO
    }
}
