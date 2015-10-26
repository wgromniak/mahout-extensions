package org.mimuw.attrsel.rules;


import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;


public class DistinguishMatrixGeneratorTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testTwoObjectInTable() {

        Matrix inputData = new DenseMatrix(new double[][]{{0, 1, 0, 1}, {1, 1, 0, 0}});
        DistinguishMatrixGenerator outpuData = new DistinguishMatrixGenerator(inputData);

        Set[][] costam = new Set[2][2];
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                costam[i][j] = new HashSet<>();
            }
        }

        costam[0][1].add(Collections.singletonList(0));
        costam[1][0].add(Collections.singletonList(0));
        for (Set[] element : costam) {
            for (Set element2 : element)
                System.out.print(element2);
            System.out.println();
        }
        assertArrayEquals(costam, outpuData.computeDistinguishMatrix());

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetFirstColumn() {
        Matrix inputData = new DenseMatrix(new double[][]{{1, 1, 0, 1, 1}, {0, 1, 0, 1, 0}, {1, 1, 1, 0, 1}, {1, 0, 0, 1, 0}});

        DistinguishMatrixGenerator outpuData = new DistinguishMatrixGenerator(inputData);

        Set[][] costam = new Set[4][4];
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                costam[i][j] = new HashSet<>();
            }
        }

        costam[0][1].add(Collections.singletonList(0));
        costam[1][0].add(Collections.singletonList(0));
        costam[0][3].add(Collections.singletonList(1));
        costam[3][0].add(Collections.singletonList(1));
        costam[1][2].add(Arrays.asList(0, 2, 3));
        costam[2][1].add(Arrays.asList(0, 2, 3));
        costam[3][2].add(Arrays.asList(1, 2, 3));
        costam[2][3].add(Arrays.asList(1, 2, 3));

        assertArrayEquals(costam, outpuData.computeDistinguishMatrix());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testEmptyInput() {
        //TODO
        /*Matrix inputData = new DenseMatrix(new double[][]{{}});

        DistinguishMatrixGenerator distinguishMatrix = new DistinguishMatrixGenerator(inputData);
        Set[][] costam = new Set[0][0];
        for (int i = 0; i < 1; i++) {
            for (int j = 0; j < 1; j++) {
                costam[i][j] = new HashSet<>();
            }
        }*/
        // assertArrayEquals(distinguishMatrix.computeDistinguishMatrix(), costam);
    }
}