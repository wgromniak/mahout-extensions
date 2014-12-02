package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MatrixFixedSizeAttributeSubtableGeneratorTest {

    @Test
    public void testName() throws Exception {

        MatrixFixedSizeAttributeSubtableGenerator matrix = new MatrixFixedSizeAttributeSubtableGenerator(new Random(),
                1, 3, new DenseMatrix(new double[][]{{1, 2, 3, 4}, {5, 6, 7, 8}}));

        List<Matrix> listOfMatrix = matrix.getSubtables();
        MatrixAssert.assertThat(listOfMatrix.get(0)).isEqualTo(new DenseMatrix(new double[][]{{1, 2, 3, 4}, {5, 6, 7, 8}}));


    }

    @Test
    public void testMockitogetSubtables() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(0, 1, 2, 3, 4, 5, 6, 7);

        MatrixFixedSizeAttributeSubtableGenerator matrixUnderTest = new MatrixFixedSizeAttributeSubtableGenerator(
                new Random(), 1, 3, new DenseMatrix(new double[][]{{1, 2, 3, 4}, {5, 6, 7, 8}}));

        List<Matrix> listOfSubtables = matrixUnderTest.getSubtables();

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2, 3, 4}, {5, 6, 7, 8}}));

    }

    @Test
    public void testTwoGetSubtables() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(0, 2, 1, 2, 0, 1);

        MatrixFixedSizeAttributeSubtableGenerator matrixUnderTest = new MatrixFixedSizeAttributeSubtableGenerator(
                random, 3, 2, new DenseMatrix(new double[][]{{1, 2, 2, 6}, {3, 4, 6, 4}, {5, 6, 8, 9}, {7, 6, 3, 6}}));

        List<Matrix> lisOfSubtables = matrixUnderTest.getSubtables();

        MatrixAssert.assertThat(lisOfSubtables.get(0)).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2, 6}, {3, 6, 4}, {5, 8, 9}, {7, 3, 6}}));
        MatrixAssert.assertThat(lisOfSubtables.get(1)).isEqualTo(
                new DenseMatrix(new double[][]{{2, 2, 6}, {4, 6, 4}, {6, 8, 9}, {6, 3, 6}}));
        MatrixAssert.assertThat(lisOfSubtables.get(2)).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2, 6}, {3, 4, 4}, {5, 6, 9}, {7, 6, 6}}));
    }
}




