
package org.mimuw.attrsel.reducts.mapred;


import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.annotations.Test;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;


public class AttrSelReducerTest {




    @Test
    public void testScore() throws Exception {
        AttrSelReducer reducer = new AttrSelReducer();
        ReduceDriver<IntWritable, IntListWritable, IntWritable, DoubleWritable> reduceDriver =
                ReduceDriver.newReduceDriver(reducer);

        List<Integer> numberOfSubtablePerAttribute = ImmutableList.of(3, 4, 5, 6);
        IntListWritable numberOfSubtablePerAttributeWritable = new IntListWritable(numberOfSubtablePerAttribute);
        File temp = File.createTempFile("numSubAttr", ".tmp");
        FileOutputStream fos = new FileOutputStream(temp);
        DataOutput dos = new DataOutputStream(fos);
        numberOfSubtablePerAttributeWritable.write(dos);

        ArrayList<IntListWritable> reducts = new ArrayList<>();
        reducts.add(new IntListWritable(ImmutableList.of(1, 3, 4)));
        reducts.add(new IntListWritable(ImmutableList.of(1, 3, 4)));
        reducts.add(new IntListWritable(ImmutableList.of(1, 3, 4)));

        int attrKey1 = 1;
        DoubleWritable score1 = new DoubleWritable(0.75);
        int attrKey2 = 2;
        DoubleWritable score2 = new DoubleWritable(0.6);

        reduceDriver.withCacheFile(temp.getAbsolutePath())
                .withInput(new IntWritable(attrKey1), reducts)
                .withOutput(new IntWritable(attrKey1), score1)
                .withInput(new IntWritable(attrKey2), reducts)
                .withOutput(new IntWritable(attrKey2), score2)
                .runTest();
    }



}