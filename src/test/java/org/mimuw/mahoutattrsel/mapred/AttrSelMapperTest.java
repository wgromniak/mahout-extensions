package org.mimuw.mahoutattrsel.mapred;


import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.AttributeSubtable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import rseslib.processing.reducts.GlobalReductsProvider;
import rseslib.processing.reducts.JohnsonReductsProvider;
import rseslib.processing.reducts.LocalReductsProvider;
import rseslib.processing.reducts.ReductsProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * To obtain result for tests testFourAttributeThreeReducts and testThreeAttributesTwoReducts I run
 * the GlobalReductProvider on input data. For each other tests I calculated results by hand.
 */
public class AttrSelMapperTest {

    private AttrSelMapper mapper;
    private MapDriver<IntWritable, SubtableWritable, IntWritable, IntListWritable> mapDriver;
    private ArrayList<Integer> listOfAttributs;
    private IntListWritable expectedOutputValue;

    Configuration conf = new Configuration();


    @BeforeMethod(groups = {"GlobalReductProvider"}, alwaysRun = true)
    public void setUp() {

        IntListWritable expectedOutputValue = new IntListWritable();

        listOfAttributs = new ArrayList();

        mapper = new AttrSelMapper();
        mapDriver = new MapDriver<IntWritable, SubtableWritable, IntWritable, IntListWritable>();
        mapDriver = MapDriver.newMapDriver(mapper);

        conf.setClass(AttrSelMapper.REDUCT_PROVIDER, GlobalReductsProvider.class, ReductsProvider.class);
        conf.set(AttrSelMapper.INDISCERNIBILITY_FOR_MISSING, "DiscernFromValue");
        conf.set(AttrSelMapper.DISCERNIBILITY_METHOD, "OrdinaryDecisionAndInconsistenciesOmitted");
        conf.set(AttrSelMapper.GRNRTSLIXEDECIISIONTRANSITIVECLOSURE, "TRUE");

        mapDriver.getConfiguration();
    }

    @Test(groups = {"GlobalReductProvider"})
    public void testOneDummyReduct() throws Exception {

        listOfAttributs.addAll(Arrays.asList(0, 1));

        expectedOutputValue = new IntListWritable(ImmutableList.of(0));

        SubtableWritable mapInputValue = new SubtableWritable(new AttributeSubtable
                (new DenseMatrix(new double[][]{{1, 1, 1}, {0, 0, 0}, {1, 0, 1}, {0, 1, 0}}), listOfAttributs, 2));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(0), expectedOutputValue);
        mapDriver.runTest();
    }

    @Test(groups = {"GlobalReductProvider"})
    public void testThreeAttributesOneReduct() throws Exception {

        listOfAttributs.addAll(Arrays.asList(0, 1, 2));

        expectedOutputValue = new IntListWritable(ImmutableList.of(2));

        SubtableWritable mapInputValue = new SubtableWritable(new AttributeSubtable(
                new DenseMatrix(new double[][]{{1, 1, 1, 1}, {0, 0, 0, 0}, {1, 1, 0, 0}, {0, 0, 0, 0}, {1, 0, 0, 0}}),
                listOfAttributs, 5));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(2), expectedOutputValue);
        mapDriver.runTest();
    }

    @Test(groups = {"GlobalReductProvider"})
    public void testFourAttributeThreeReducts() throws Exception {

        listOfAttributs.addAll(Arrays.asList(0, 1, 2, 3));

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


        SubtableWritable mapInputValue = new SubtableWritable(new AttributeSubtable(new DenseMatrix(
                new double[][]{{1, 0, 2, 0, 1}, {0, 1, 0, 2, 0}, {1, 0, 0, 0, 1},
                        {0, 1, 1, 0, 1}, {0, 0, 1, 0, 1}, {1, 0, 0, 0, 1}}), listOfAttributs, 10));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(3), toOutFirst);
        mapDriver.withOutput(new IntWritable(1), toOutSecond);
        mapDriver.withOutput(new IntWritable(2), toOutThird);
        mapDriver.withOutput(new IntWritable(0), toOutFourth);
        mapDriver.withOutput(new IntWritable(2), toOutFifth);
        mapDriver.runTest();
    }


    @Test(groups = {"GlobalReductProvider"})
    public void testThreeAttributesTwoReducts() throws Exception {

        listOfAttributs.addAll(Arrays.asList(0, 1, 2));

        ArrayList<List<Integer>> multipleOut = new ArrayList<>();

        multipleOut.add(0, Arrays.asList(2));
        multipleOut.add(1, Arrays.asList(0, 1));
        multipleOut.add(2, Arrays.asList(0, 1));

        IntListWritable toOutFirst = new IntListWritable(multipleOut.get(0));
        IntListWritable toOutSecond = new IntListWritable(multipleOut.get(1));
        IntListWritable toOutThird = new IntListWritable(multipleOut.get(2));

        SubtableWritable mapInputValue = new SubtableWritable(new AttributeSubtable(new DenseMatrix(
                new double[][]{{1, 2, 0, 1}, {0, 2, 2, 0}, {1, 1, 0, 1}, {0, 1, 0, 1},
                        {0, 2, 2, 0}, {1, 1, 0, 1}, {1, 1, 1, 1}}), listOfAttributs, 3333));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(2), toOutFirst);
        mapDriver.withOutput(new IntWritable(0), toOutSecond);
        mapDriver.withOutput(new IntWritable(1), toOutThird);
        mapDriver.runTest();
    }

    @Test(groups = {"GlobalReductProvider"})
    public void testAnotherAttribute() throws Exception {

        listOfAttributs.addAll(Arrays.asList(2, 5433, 4));

        expectedOutputValue = new IntListWritable(ImmutableList.of(4));

        Matrix decisionMatrix = new DenseMatrix(new double[][]{{1, 1, 1, 1}, {0, 0, 0, 0}, {1, 1, 0, 0},
                {0, 0, 0, 0}, {1, 0, 0, 0}});

        SubtableWritable mapInputValue = new SubtableWritable(new AttributeSubtable(
                decisionMatrix, listOfAttributs, 10));


        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(4), expectedOutputValue);
        mapDriver.runTest();
    }

    @Test(groups = {"GlobalReductProvider"})
    public void testOneReduct() throws Exception {

        listOfAttributs.addAll(Arrays.asList(2, 3));

        expectedOutputValue = new IntListWritable(ImmutableList.of(2));

        SubtableWritable mapInputValue = new SubtableWritable(new AttributeSubtable
                (new DenseMatrix(new double[][]{{1, 1, 1}, {1, 1, 1}, {0, 0, 0}, {0, 1, 0}, {1, 0, 1}}),
                        listOfAttributs, 4));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(2), expectedOutputValue);
        mapDriver.runTest();
    }

    @Test(groups = {"LocalReductProvider"})
    public void testOneReductLocal() throws Exception {

        IntListWritable expectedOutputValue = new IntListWritable();

        listOfAttributs = new ArrayList();

        mapper = new AttrSelMapper();
        mapDriver = new MapDriver<IntWritable, SubtableWritable, IntWritable, IntListWritable>();
        mapDriver = MapDriver.newMapDriver(mapper);

        conf.setClass(AttrSelMapper.REDUCT_PROVIDER, LocalReductsProvider.class, ReductsProvider.class);
        conf.set(AttrSelMapper.INDISCERNIBILITY_FOR_MISSING, "DiscernFromValue");
        conf.set(AttrSelMapper.DISCERNIBILITY_METHOD, "OrdinaryDecisionAndInconsistenciesOmitted");
        conf.set(AttrSelMapper.GRNRTSLIXEDECIISIONTRANSITIVECLOSURE, "TRUE");
        mapDriver.getConfiguration();

        listOfAttributs.addAll(Arrays.asList(2, 3));

        expectedOutputValue = new IntListWritable(ImmutableList.of(2));

        SubtableWritable mapInputValue = new SubtableWritable(new AttributeSubtable
                (new DenseMatrix(new double[][]{{1, 1, 1}, {1, 1, 1}, {0, 0, 0}, {0, 1, 0}, {1, 0, 1}}),
                        listOfAttributs, 4));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(2), expectedOutputValue);
        mapDriver.runTest();
    }

    @Test(groups = {"JohnsonReductProvider"})
    public void testOneReductJohnson() throws Exception {

        IntListWritable expectedOutputValue = new IntListWritable();

        listOfAttributs = new ArrayList();

        mapper = new AttrSelMapper();
        mapDriver = new MapDriver<IntWritable, SubtableWritable, IntWritable, IntListWritable>();
        mapDriver = MapDriver.newMapDriver(mapper);

        conf.setClass(AttrSelMapper.REDUCT_PROVIDER, JohnsonReductsProvider.class, ReductsProvider.class);
        conf.set(AttrSelMapper.INDISCERNIBILITY_FOR_MISSING, "DiscernFromValue");
        conf.set(AttrSelMapper.DISCERNIBILITY_METHOD, "OrdinaryDecisionAndInconsistenciesOmitted");
        conf.set(AttrSelMapper.GRNRTSLIXEDECIISIONTRANSITIVECLOSURE, "TRUE");
        conf.set(AttrSelMapper.JOHNSON_REDUCT, "One");
        mapDriver.getConfiguration();

        listOfAttributs.addAll(Arrays.asList(2, 3));

        expectedOutputValue = new IntListWritable(ImmutableList.of(2));

        SubtableWritable mapInputValue = new SubtableWritable(new AttributeSubtable
                (new DenseMatrix(new double[][]{{1, 1, 1}, {1, 1, 1}, {0, 0, 0}, {0, 1, 0}, {1, 0, 1}}),
                        listOfAttributs, 4));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(2), expectedOutputValue);
        mapDriver.runTest();
    }
}
