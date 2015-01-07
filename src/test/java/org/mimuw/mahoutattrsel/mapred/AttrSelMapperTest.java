package org.mimuw.mahoutattrsel.mapred;


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
 * The result for tests testFourAttributeThreeReducts, testThreeAttributesTwoReducts and testOneReductJohnson were
 * obtained by running the GlobalReductProvder on input data. For the rest of test the result were obtained by hand.
 */
public class AttrSelMapperTest {

    private MapDriver<IntWritable, SubtableWritable, IntWritable, IntListWritable> mapDriver;
    private Configuration conf = new Configuration();


    @BeforeMethod
    public void setUp() {

        AttrSelMapper mapper = new AttrSelMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        conf = mapDriver.getConfiguration();
        conf.setClass(AttrSelMapper.REDUCT_PROVIDER, GlobalReductsProvider.class, ReductsProvider.class);
        conf.set(AttrSelMapper.INDISCERNIBILITY_FOR_MISSING, "DiscernFromValue");
        conf.set(AttrSelMapper.DISCERNIBILITY_METHOD, "OrdinaryDecisionAndInconsistenciesOmitted");
        conf.set(AttrSelMapper.GENERALIZED_DECISION_TRANSITIVE_CLOSURE, "TRUE");
    }

    @Test
    public void testOneDummyReduct() throws Exception {

        List<Integer> listOfAttributes = Arrays.asList(0, 1);

        IntListWritable expectedOutputValue = new IntListWritable(Arrays.asList(0));

        SubtableWritable mapInputValue = new SubtableWritable(new AttributeSubtable
                (new DenseMatrix(new double[][]{{1, 1, 1}, {0, 0, 0}, {1, 0, 1}, {0, 1, 0}}), listOfAttributes, 2));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(0), expectedOutputValue);
        mapDriver.runTest();
    }

    @Test
    public void testThreeAttributesOneReduct() throws Exception {


        List<Integer> listOfAttributes = Arrays.asList(0, 1, 2);

        IntListWritable expectedOutputValue = new IntListWritable(Arrays.asList(2));

        SubtableWritable mapInputValue = new SubtableWritable(new AttributeSubtable(
                new DenseMatrix(new double[][]{{1, 1, 1, 1}, {0, 0, 0, 0}, {1, 1, 0, 0}, {0, 0, 0, 0}, {1, 0, 0, 0}}),
                listOfAttributes, 5));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(2), expectedOutputValue);
        mapDriver.runTest();
    }

    @Test
    public void testFourAttributeThreeReducts() throws Exception {

        List<Integer> listOfAttributes = Arrays.asList(0, 1, 2, 3);

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
                        {0, 1, 1, 0, 1}, {0, 0, 1, 0, 1}, {1, 0, 0, 0, 1}}),
                listOfAttributes, 10));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(3), toOutFirst);
        mapDriver.withOutput(new IntWritable(1), toOutSecond);
        mapDriver.withOutput(new IntWritable(2), toOutThird);
        mapDriver.withOutput(new IntWritable(0), toOutFourth);
        mapDriver.withOutput(new IntWritable(2), toOutFifth);
        mapDriver.runTest();
    }


    @Test
    public void testThreeAttributesTwoReducts() throws Exception {


        List<Integer> listOfAttributes = Arrays.asList(0, 1, 2);

        ArrayList<List<Integer>> multipleOut = new ArrayList<>();

        multipleOut.add(0, Arrays.asList(2));
        multipleOut.add(1, Arrays.asList(0, 1));
        multipleOut.add(2, Arrays.asList(0, 1));

        IntListWritable toOutFirst = new IntListWritable(multipleOut.get(0));
        IntListWritable toOutSecond = new IntListWritable(multipleOut.get(1));
        IntListWritable toOutThird = new IntListWritable(multipleOut.get(2));

        Matrix decisionMatrix = new DenseMatrix(new double[][]{{1, 2, 0, 1}, {0, 2, 2, 0}, {1, 1, 0, 1}, {0, 1, 0, 1},
                {0, 2, 2, 0}, {1, 1, 0, 1}, {1, 1, 1, 1}});

        SubtableWritable mapInputValue = new SubtableWritable(
                new AttributeSubtable(decisionMatrix, listOfAttributes, 3333));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(2), toOutFirst);
        mapDriver.withOutput(new IntWritable(0), toOutSecond);
        mapDriver.withOutput(new IntWritable(1), toOutThird);
        mapDriver.runTest();
    }

    @Test
    public void testAnotherAttribute() throws Exception {


        List<Integer> listOfAttributes = Arrays.asList(2, 5433, 4);

        IntListWritable expectedOutputValue = new IntListWritable(Arrays.asList(4));

        Matrix decisionMatrix = new DenseMatrix(new double[][]{{1, 1, 1, 1}, {0, 0, 0, 0}, {1, 1, 0, 0},
                {0, 0, 0, 0}, {1, 0, 0, 0}});

        SubtableWritable mapInputValue = new SubtableWritable(
                new AttributeSubtable(decisionMatrix, listOfAttributes, 10));


        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(4), expectedOutputValue);
        mapDriver.runTest();
    }

    @Test
    public void testOneReduct() throws Exception {

        List<Integer> listOfAttributes = Arrays.asList(2, 3);

        IntListWritable expectedOutputValue = new IntListWritable(Arrays.asList(2));

        SubtableWritable mapInputValue = new SubtableWritable(new AttributeSubtable
                (new DenseMatrix(new double[][]{{1, 1, 1}, {1, 1, 1}, {0, 0, 0}, {0, 1, 0}, {1, 0, 1}}),
                        listOfAttributes, 4));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(2), expectedOutputValue);
        mapDriver.runTest();
    }

    @Test
    public void testOneReductLocal() throws Exception {

        conf = mapDriver.getConfiguration();
        conf.setClass(AttrSelMapper.REDUCT_PROVIDER, LocalReductsProvider.class, ReductsProvider.class);
        conf.set(AttrSelMapper.INDISCERNIBILITY_FOR_MISSING, "DiscernFromValue");
        conf.set(AttrSelMapper.DISCERNIBILITY_METHOD, "OrdinaryDecisionAndInconsistenciesOmitted");
        conf.set(AttrSelMapper.GENERALIZED_DECISION_TRANSITIVE_CLOSURE, "TRUE");

        List<Integer> listOfAttributes = Arrays.asList(2, 3);

        IntListWritable expectedOutputValue = new IntListWritable(Arrays.asList(2));

        SubtableWritable mapInputValue = new SubtableWritable(new AttributeSubtable
                (new DenseMatrix(new double[][]{{1, 1, 1}, {1, 1, 1}, {0, 0, 0}, {0, 1, 0}, {1, 0, 1}}),
                        listOfAttributes, 4));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(2), expectedOutputValue);
        mapDriver.withOutput(new IntWritable(2), expectedOutputValue);
        mapDriver.withOutput(new IntWritable(2), expectedOutputValue);
        mapDriver.withOutput(new IntWritable(2), expectedOutputValue);
        mapDriver.withOutput(new IntWritable(2), expectedOutputValue);
        mapDriver.runTest();
    }

    @Test
    public void testOneReductJohnson() throws Exception {

        conf = mapDriver.getConfiguration();
        conf.setClass(AttrSelMapper.REDUCT_PROVIDER, JohnsonReductsProvider.class, ReductsProvider.class);
        conf.set(AttrSelMapper.INDISCERNIBILITY_FOR_MISSING, "DiscernFromValue");
        conf.set(AttrSelMapper.DISCERNIBILITY_METHOD, "OrdinaryDecisionAndInconsistenciesOmitted");
        conf.set(AttrSelMapper.GENERALIZED_DECISION_TRANSITIVE_CLOSURE, "TRUE");
        conf.set(AttrSelMapper.JOHNSON_REDUCTS, "One");

        List<Integer> listOfAttributes = Arrays.asList(0, 1, 2, 3);

        IntListWritable expectedOutputValue = new IntListWritable(Arrays.asList(3));

        SubtableWritable mapInputValue = new SubtableWritable(new AttributeSubtable
                (new DenseMatrix(new double[][]{{1, 1, 2, 3, 4}, {0, 0, 3, 1, 0}, {1, 0, 5, 6, 1}, {0, 1, 2, 4, 0}}),
                        listOfAttributes, 10560));

        mapDriver.withInput(new IntWritable(0), mapInputValue);
        mapDriver.withOutput(new IntWritable(3), expectedOutputValue);
        mapDriver.runTest();
    }
}
