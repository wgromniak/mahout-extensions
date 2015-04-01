package org.mimuw.attrsel.trees.mapred;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mimuw.attrsel.common.SubtableWritable;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.trees.MCFS;

import java.io.IOException;
import java.util.Random;

/**
 * Input - ( no of subtable, subtable ), output - ( attribute, score ).
 */
public class TreeAttrSelMapper extends Mapper<IntWritable, SubtableWritable, IntWritable, DoubleWritable> {

    @Override
    protected void map(IntWritable key, SubtableWritable value, Context context)
            throws IOException, InterruptedException {

        Subtable subtable = value.get();

        // TODO: values hardcoded temporarily
        MCFS mcfs = new MCFS(10, new Random(1234), 2, 2);

        double[] scores = mcfs.getScores(subtable.getTable());

        for (int i = 0; i < scores.length; i++) {

            context.write(new IntWritable(subtable.getAttributeAtPosition(i)), new DoubleWritable(scores[i]));
        }
    }
}
