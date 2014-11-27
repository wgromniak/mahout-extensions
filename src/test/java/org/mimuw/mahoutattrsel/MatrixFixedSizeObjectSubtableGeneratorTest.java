package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Random;

import static org.testng.Assert.assertEquals;

public class MatrixFixedSizeObjectSubtableGeneratorTest {

    @Test
    public void testconvertMatrix() throws Exception {

        ArrayList<Vector> inputVectorList = new ArrayList<>();
        inputVectorList.add(new DenseVector(new double[]{1,2,3}));
        inputVectorList.add(new DenseVector(new double[]{7,6,5}));
        Matrix outputTest = new DenseMatrix(3,3);
        MatrixFixedSizeObjectSubtableGenerator generatorUnderTest = new MatrixFixedSizeObjectSubtableGenerator(new Random(),2,1,outputTest);
        Matrix outputMatrix = generatorUnderTest.convertToMatrix(inputVectorList);

        assertEquals(outputMatrix,new DenseMatrix(new double[][]{{1,2,3},{7,6,5}}));

    }
}