package org.mimuw.attrsel.trees.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mimuw.attrsel.common.SubtableWritable;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.trees.MCFS;

import java.io.IOException;

/**
 * Input - ( no of subtable, subtable ), output - ( attribute, score ).
 *
 * TODO: add tests
 */
public class TreeAttrSelMapper extends Mapper<IntWritable, SubtableWritable, IntWritable, DoubleWritable> {

    private MCFS mcfs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        mcfs = new MCFS(
                conf.getInt("numberOfTrees", 100),
                conf.getLong("seed", 123456789),
                conf.getDouble("u", 2),
                conf.getDouble("v", 2)
        );
    }

    @Override
    protected void map(IntWritable key, SubtableWritable value, Context context)
            throws IOException, InterruptedException {

        Subtable subtable = value.get();

        double[] scores = mcfs.getScores(subtable.getTable());

        for (int i = 0; i < scores.length; i++) {

            context.write(new IntWritable(subtable.getAttributeAtPosition(i)), new DoubleWritable(scores[i]));
        }
    }
}
