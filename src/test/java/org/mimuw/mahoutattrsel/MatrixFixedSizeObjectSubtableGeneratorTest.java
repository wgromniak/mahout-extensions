package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MatrixFixedSizeObjectSubtableGeneratorTest {

    @Test
    public void testSimple() throws Exception {

        MatrixFixedSizeObjectSubtableGenerator matrix = new MatrixFixedSizeObjectSubtableGenerator(new Random(), 2, 3,
                new DenseMatrix(new double[][]{{1, 2, 3}, {7, 6, 5}, {3, 3, 3}}));

        List<Matrix> listOfSubtables = matrix.getSubtables();

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2, 3}, {7, 6, 5}, {3, 3, 3}}));
        MatrixAssert.assertThat(listOfSubtables.get(1)).isEqualTo
                (new DenseMatrix(new double[][]{{1, 2, 3}, {7, 6, 5}, {3, 3, 3}}));

    }

    @Test
    public void testGetSubtablesWithStubbedRandom() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(0, 1);

        MatrixFixedSizeObjectSubtableGenerator matrix = new MatrixFixedSizeObjectSubtableGenerator(random, 1, 2,
                new DenseMatrix(new double[][]{{1, 2, 3}, {7, 6, 5}}));

        List<Matrix> listOfSubtables = matrix.getSubtables();

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(new DenseMatrix
                (new double[][]{{1, 2, 3}, {7, 6, 5}}));

    }

    @Test
    public void testOneSubtableWithThreeObjectsPredictableRandom() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(0, 1, 2);
        MatrixFixedSizeObjectSubtableGenerator matrixUnderTest = new MatrixFixedSizeObjectSubtableGenerator(
                random, 1, 3, new DenseMatrix(new double[][]{{1, 2}, {3, 4}, {5, 6}, {7, 6}, {8, 9}}));

        List<Matrix> listOfSubtables = matrixUnderTest.getSubtables();

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2}, {3, 4}, {5, 6}}));
    }

    @Test
    public void testThreeSubtableWithFourObjectsPredictableRandom() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(0, 1, 2, 3, 0, 1, 2, 4, 4, 2, 1, 0);
        MatrixFixedSizeObjectSubtableGenerator matrixUnderTest = new MatrixFixedSizeObjectSubtableGenerator(
                random, 3, 4, new DenseMatrix(new double[][]{{1, 2}, {3, 4}, {5, 6}, {7, 6}, {8, 9}}));

        List<Matrix> listOfSubtables = matrixUnderTest.getSubtables();

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2}, {3, 4}, {5, 6}, {7, 6}}));

        MatrixAssert.assertThat(listOfSubtables.get(1)).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2}, {3, 4}, {5, 6}, {8, 9}}));

        MatrixAssert.assertThat(listOfSubtables.get(2)).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2}, {3, 4}, {5, 6}, {8, 9}}));
    }

    @Test
    public void testOneDimensionalGetSubtable() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(0, 1, 2);

        MatrixFixedSizeObjectSubtableGenerator matrix = new MatrixFixedSizeObjectSubtableGenerator(random, 2, 1,
                new DenseMatrix(new double[][]{{1, 2, 3}, {4, 5, 6}}));

        List<Matrix> listOfSubtables = matrix.getSubtables();

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(new DenseMatrix(new double[][]{{1, 2, 3}}));
        MatrixAssert.assertThat(listOfSubtables.get(1)).isEqualTo(new DenseMatrix(new double[][]{{4, 5, 6}}));
    }
}
