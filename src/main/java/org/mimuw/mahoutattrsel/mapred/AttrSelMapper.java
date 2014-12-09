package org.mimuw.mahoutattrsel.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Input - ( no of subtable, subtable ), output - ( attribute, reduct ).
 */
public final class AttrSelMapper extends Mapper<IntWritable, SubtableWritable, IntWritable, IntListWritable> {

    @Override
    protected void map(IntWritable key, SubtableWritable value, Context context)
            throws IOException, InterruptedException {
        // TODO
    }
}
