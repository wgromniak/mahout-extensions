package org.mimuw.mahoutattrsel.mapred;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.mahout.math.DenseMatrix;
import org.mimuw.mahoutattrsel.AttributeSubtable;
import org.testng.annotations.Test;

import java.util.ArrayList;


public class AttrSelMapperTest {


    @Test
    public void testName() throws Exception {

        AttrSelMapper mapper = new AttrSelMapper();
        MapDriver<IntWritable, SubtableWritable, IntWritable, IntListWritable> mapDriver;
        mapDriver = new MapDriver<IntWritable, SubtableWritable, IntWritable, IntListWritable>();
        mapDriver = MapDriver.newMapDriver(mapper);


        ArrayList<Integer> tmp = new ArrayList<>();
        tmp.add(0);
        tmp.add(1);

        ArrayList<Integer> out = new ArrayList<>();
        out.add(0);

        IntListWritable toOut = new IntListWritable(out);

        SubtableWritable ex = new SubtableWritable(new AttributeSubtable
                (new DenseMatrix(new double[][]{{1, 1, 1}, {0, 0, 0}, {1, 0, 1}, {0, 1, 0}}), tmp, 2));

        System.out.println(toOut);
        mapDriver.withInput(new IntWritable(0), ex);
        mapDriver.withOutput(new IntWritable(0), toOut);
        mapDriver.runTest();
    }

}