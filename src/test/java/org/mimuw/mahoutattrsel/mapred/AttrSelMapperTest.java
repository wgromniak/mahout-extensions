package org.mimuw.mahoutattrsel.mapred;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.mahout.math.DenseMatrix;
import org.mimuw.mahoutattrsel.AttributeSubtable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import rseslib.processing.reducts.GlobalReductsProvider;
import rseslib.processing.reducts.ReductsProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class AttrSelMapperTest {

    AttrSelMapper mapper;
    MapDriver<IntWritable, SubtableWritable, IntWritable, IntListWritable> mapDriver;
    ArrayList<Integer> listOfAttributs;

    Configuration conf = new Configuration();


    @BeforeMethod
    public void setUp() {

        mapper = new AttrSelMapper();
        mapDriver = new MapDriver<IntWritable, SubtableWritable, IntWritable, IntListWritable>();
        mapDriver = MapDriver.newMapDriver(mapper);

        listOfAttributs = new ArrayList<>();

        conf.setClass(AttrSelMapper.REDUCT_PROVIDER, GlobalReductsProvider.class, ReductsProvider.class);
        conf.set("IndiscernibilityForMissing", AttrSelMapper.INDISCERNIBILITY_FOR_MISSING);
        conf.set("DiscernibilityMethod", AttrSelMapper.DISCERNIBILITY_METHOD);
        conf.set("GeneralizedDecisionTransitiveClosure", AttrSelMapper.GeneralizedDecisionTransitiveClosure);
    }

    @Test
    public void testOneDummyReduct() throws Exception {

        ArrayList<Integer> out = new ArrayList<>();

        listOfAttributs.add(0);
        listOfAttributs.add(1);
        out.add(0);

        IntListWritable toOut = new IntListWritable(out);

        SubtableWritable ex = new SubtableWritable(new AttributeSubtable
                (new DenseMatrix(new double[][]{{1, 1, 1}, {0, 0, 0}, {1, 0, 1}, {0, 1, 0}}), listOfAttributs, 2));

        mapDriver.setConfiguration(conf);
        mapDriver.withInput(new IntWritable(0), ex);
        mapDriver.withOutput(new IntWritable(0), toOut);
        mapDriver.runTest();
    }

    @Test
    public void testThreeAttributesOneReduct() throws Exception {

        ArrayList<Integer> out = new ArrayList<>();

        for (int i = 0; i < 3; i++) {

            listOfAttributs.add(i);
        }

        out.add(2);

        IntListWritable toOut = new IntListWritable(out);

        SubtableWritable toMap = new SubtableWritable(new AttributeSubtable(
                new DenseMatrix(new double[][]
                        {{1, 1, 1, 1}, {0, 0, 0, 0}, {1, 1, 0, 0}, {0, 0, 0, 0}, {1, 0, 0, 0}}), listOfAttributs, 3));

        mapDriver.setConfiguration(conf);
        mapDriver.withInput(new IntWritable(0), toMap);
        mapDriver.withOutput(new IntWritable(2), toOut);
        mapDriver.runTest();
    }

    @Test
    public void testFourAttributeThreeReducts() throws Exception {

        for (int i = 0; i < 4; i++) {

            listOfAttributs.add(i);
        }

        ArrayList<List<Integer>> multipleOut = new ArrayList<>();

        multipleOut.add(0, Arrays.asList(3));
        multipleOut.add(1, Arrays.asList(1, 2));
        multipleOut.add(2, Arrays.asList(1, 2));
        multipleOut.add(3, Arrays.asList(0, 2));
        multipleOut.add(4, Arrays.asList(0, 2));

        IntListWritable toOutFirst = new IntListWritable(multipleOut.get(0));
        IntListWritable toOutSecond = new IntListWritable(multipleOut.get(1));
        IntListWritable toOutThird = new IntListWritable(multipleOut.get(2));
        IntListWritable toOutFourth = new IntListWritable(multipleOut.get(3));
        IntListWritable toOutFifth = new IntListWritable(multipleOut.get(4));


        SubtableWritable toMap = new SubtableWritable(new AttributeSubtable(
                new DenseMatrix(new double[][]
                        {{1, 0, 2, 0, 1}, {0, 1, 0, 2, 0}, {1, 0, 0, 0, 1}, {0, 1, 1, 0, 1},
                                {0, 0, 1, 0, 1}, {1, 0, 0, 0, 1}}), listOfAttributs, 4));

        mapDriver.setConfiguration(conf);
        mapDriver.withInput(new IntWritable(0), toMap);
        mapDriver.withOutput(new IntWritable(3), toOutFirst);
        mapDriver.withOutput(new IntWritable(1), toOutSecond);
        mapDriver.withOutput(new IntWritable(2), toOutThird);
        mapDriver.withOutput(new IntWritable(0), toOutFourth);
        mapDriver.withOutput(new IntWritable(2), toOutFifth);
        mapDriver.runTest();

    }


    @Test
    public void testThreeAttributesTwoReducts() throws Exception {

        for (int i = 0; i < 3; i++) {

            listOfAttributs.add(i);
        }

        ArrayList<List<Integer>> multipleOut = new ArrayList<>();

        multipleOut.add(0, Arrays.asList(2));
        multipleOut.add(1, Arrays.asList(0, 1));
        multipleOut.add(2, Arrays.asList(0, 1));

        IntListWritable toOutFirst = new IntListWritable(multipleOut.get(0));
        IntListWritable toOutSecond = new IntListWritable(multipleOut.get(1));
        IntListWritable toOutThird = new IntListWritable(multipleOut.get(2));

        SubtableWritable toMap = new SubtableWritable(new AttributeSubtable(
                new DenseMatrix(new double[][]
                        {{1, 2, 0, 1}, {0, 2, 2, 0}, {1, 1, 0, 1}, {0, 1, 0, 1},
                                {0, 2, 2, 0}, {1, 1, 0, 1}, {1, 1, 1, 1}}), listOfAttributs, 3));
        mapDriver.setConfiguration(conf);
        mapDriver.withInput(new IntWritable(0), toMap);
        mapDriver.withOutput(new IntWritable(2), toOutFirst);
        mapDriver.withOutput(new IntWritable(0), toOutSecond);
        mapDriver.withOutput(new IntWritable(1), toOutThird);
        mapDriver.runTest();
    }
}
