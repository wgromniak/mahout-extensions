package org.mimuw.attrsel.rules;


import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.testng.annotations.Test;

import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class DistinguishMatrixGeneratorTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testTwoObjectInTable() {

        Matrix inputData = new DenseMatrix(new double[][]{{0, 1, 0, 1}, {1, 1, 0, 0}});
        DistinguishMatrixGenerator outputData = new DistinguishMatrixGenerator(inputData);

        Set<Integer>[][] distinguishMatrix = new Set[2][2];
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                distinguishMatrix[i][j] = new HashSet<>();
            }
        }

        distinguishMatrix[0][1].addAll(Collections.singletonList(0));
        distinguishMatrix[1][0].addAll(Collections.singletonList(0));
        assertArrayEquals(distinguishMatrix, outputData.computeDistinguishMatrix());

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetFirstColumn() {
        Matrix inputData = new DenseMatrix(new double[][]{{1, 1, 0, 1, 1}, {0, 1, 0, 1, 0},
                {1, 1, 1, 0, 1}, {1, 0, 0, 1, 0}});
        DistinguishMatrixGenerator output = new DistinguishMatrixGenerator(inputData);

        Set<Integer>[][] distinguishTable = new Set[4][4];
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                distinguishTable[i][j] = new HashSet<>();
            }
        }

        distinguishTable[0][1].addAll(Collections.singletonList(0));
        distinguishTable[1][0].addAll(Collections.singletonList(0));
        distinguishTable[0][3].addAll(Collections.singletonList(1));
        distinguishTable[3][0].addAll(Collections.singletonList(1));
        distinguishTable[1][2].addAll(Arrays.asList(0, 2, 3));
        distinguishTable[2][1].addAll(Arrays.asList(0, 2, 3));
        distinguishTable[3][2].addAll(Arrays.asList(1, 2, 3));
        distinguishTable[2][3].addAll(Arrays.asList(1, 2, 3));

        assertArrayEquals(distinguishTable, output.computeDistinguishMatrix());
    }

    @Test(expectedExceptions=IllegalArgumentException.class)
    public void testEmptyInput() {
        Matrix inputData =  new DenseMatrix(new double[][]{{}});
        DistinguishMatrixGenerator distinguishMatrix = new DistinguishMatrixGenerator(inputData);
        Set[][] expectedDistinguishMatrix = new Set[inputData.rowSize()][inputData.rowSize()];
        for (int i = 0; i < inputData.rowSize(); ++i) {
            for (int j = 0; j < inputData.rowSize(); ++j) {
                expectedDistinguishMatrix[i][j] = new HashSet<>();
            }
        }
        assertArrayEquals(distinguishMatrix.computeDistinguishMatrix(), expectedDistinguishMatrix);
    }

    @Test
    public void oneObjectInput() {
        Matrix inputData =  new DenseMatrix(new double[][]{{1,2,3,1,0}});
        DistinguishMatrixGenerator distinguishMatrix = new DistinguishMatrixGenerator(inputData);
        Set[][] expectedDistinguishMatrix = new Set[inputData.rowSize()][inputData.rowSize()];
        for (int i = 0; i < inputData.rowSize(); ++i) {
            for (int j = 0; j < inputData.rowSize(); ++j) {
                expectedDistinguishMatrix[i][j] = new HashSet<>();
            }
        }

        assertArrayEquals(distinguishMatrix.computeDistinguishMatrix(), expectedDistinguishMatrix);
//        Set[][] a =expectedDistinguishMatrix;
//        for (Set[] element : a) {
//            for (Set element2 : element)
//                System.out.print(element2);
//            System.out.println();
//        }

    }

    @SuppressWarnings("unchecked")
    @Test
    public void twoObjectsFirstGetColumn(){
        Matrix inputData = new DenseMatrix(new double[][]{{1,2,3,4,0},{5,6,7,8,1}, {1,2,1,4,1}});
        DistinguishMatrixGenerator distinguishMatrix = new DistinguishMatrixGenerator(inputData);
        distinguishMatrix.computeDistinguishMatrix();
        ArrayList<Set> firstColumn =  distinguishMatrix.getColumn(0);
        ArrayList<Set> expectedOutput = new ArrayList<>();

        expectedOutput.add(new HashSet());
        expectedOutput.add(new HashSet(Arrays.asList(0, 1, 2, 3)));
        expectedOutput.add(new HashSet(Collections.singletonList(2)));
        assertEquals(expectedOutput, firstColumn);
    }

}
