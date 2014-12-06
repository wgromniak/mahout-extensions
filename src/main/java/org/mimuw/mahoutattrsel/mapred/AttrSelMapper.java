package org.mimuw.mahoutattrsel.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.MatrixWritable;

import java.io.IOException;

/**
 * Input - ( no of subtable, subtable ), output - ( attribute, reduct ).
 */
public final class AttrSelMapper extends Mapper<IntWritable, MatrixWritable, Text, BitSetWritable> {

    @Override
    protected void map(IntWritable key, MatrixWritable value, Context context)
            throws IOException, InterruptedException {
        // TODO
    }
}
