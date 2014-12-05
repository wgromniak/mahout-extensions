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
    public void testThreeSubtableOneAttribute() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(0, 1, 2, 3);

        MatrixFixedSizeAttributeSubtableGenerator matrixUnderTest = new MatrixFixedSizeAttributeSubtableGenerator(
                random, 3, 1, new DenseMatrix(new double[][]{{1, 2, 2, 6}, {3, 4, 6, 4}, {5, 6, 8, 9}, {7, 6, 3, 6}}));

        List<Matrix> listOfSubtables = matrixUnderTest.getSubtables();

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(
                new DenseMatrix(new double[][]{{1, 6}, {3, 4}, {5, 9}, {7, 6}}));
        MatrixAssert.assertThat(listOfSubtables.get(1)).isEqualTo(
                new DenseMatrix(new double[][]{{2, 6}, {4, 4}, {6, 9}, {6, 6}}));
        MatrixAssert.assertThat(listOfSubtables.get(2)).isEqualTo(
                new DenseMatrix(new double[][]{{2, 6}, {6, 4}, {8, 9}, {3, 6}}));

    }

    @Test
    public void testOneSubtablesThreeAttributes() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(0, 1, 2, 3, 4, 5, 6, 7);

        MatrixFixedSizeAttributeSubtableGenerator matrixUnderTest = new MatrixFixedSizeAttributeSubtableGenerator(
                new Random(), 1, 3, new DenseMatrix(new double[][]{{1, 2, 3, 4}, {5, 6, 7, 8}}));

        List<Matrix> listOfSubtables = matrixUnderTest.getSubtables();

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2, 3, 4}, {5, 6, 7, 8}}));

    }

    @Test
    public void testThreeSubtablesTwoAttributes() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(1, 0, 2, 2, 1, 1);

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

    @Test
    public void testThreeSubtablesTwoAttributesSeed() throws Exception {

        Random random = new Random(100);

        MatrixFixedSizeAttributeSubtableGenerator matrixUnderTest = new MatrixFixedSizeAttributeSubtableGenerator(
                random, 3, 2, new DenseMatrix(new double[][]{{1, 2, 3, 4, 5}, {4, 4, 5, 1, 3}, {5, 4, 2, 1, 3},
                {4, 2, 1, 3, 4}, {5, 6, 7, 4, 1}, {4, 44, 21, 2, 3}}));

        List<Matrix> listOfSubtables = matrixUnderTest.getSubtables();

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(new DenseMatrix(
                new double[][]{{1, 3, 5}, {4, 5, 3}, {5, 2, 3}, {4, 1, 4}, {5, 7, 1}, {4, 21, 3}}));

        MatrixAssert.assertThat(listOfSubtables.get(1)).isEqualTo(new DenseMatrix(
                new double[][]{{1, 3, 5}, {4, 5, 3}, {5, 2, 3}, {4, 1, 4}, {5, 7, 1}, {4, 21, 3}}));

        MatrixAssert.assertThat(listOfSubtables.get(2)).isEqualTo(new DenseMatrix(
                new double[][]{{2, 4, 5}, {4, 1, 3}, {4, 1, 3}, {2, 3, 4}, {6, 4, 1}, {44, 2, 3}}));
    }

    @Test
    public void testTwoSubtablesThreeAttributesSeed() throws Exception {

        Random random = new Random(36);

        MatrixFixedSizeAttributeSubtableGenerator matrixUnderTest = new MatrixFixedSizeAttributeSubtableGenerator(
                random, 2, 3, new DenseMatrix(new double[][]{{1, 2, 3, 4, 5}, {4, 4, 5, 1, 3}, {5, 4, 2, 1, 3},
                {4, 2, 1, 3, 4}, {5, 6, 7, 4, 1}, {4, 44, 21, 2, 3}}));

        List<Matrix> listOfSubtables = matrixUnderTest.getSubtables();

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(new DenseMatrix(new double[][]
                {{1, 2, 4, 5}, {4, 4, 1, 3}, {5, 4, 1, 3},
                        {4, 2, 3, 4}, {5, 6, 4, 1}, {4, 44, 2, 3}}));

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(new DenseMatrix(new double[][]
                {{1, 2, 4, 5}, {4, 4, 1, 3}, {5, 4, 1, 3},
                        {4, 2, 3, 4}, {5, 6, 4, 1}, {4, 44, 2, 3}}));

    }

    @Test
    public void testName() throws Exception {

        Random random = new Random(175);

        MatrixFixedSizeAttributeSubtableGenerator matrixUnderTests = new MatrixFixedSizeAttributeSubtableGenerator(
                random, 5, 1, new DenseMatrix(new double[][]{{4, 3, 3, 2}, {1, 2, 3, 4}, {5, 4, 3, 2},
                {4, 3, 2, 1}, {4, 2, 2, 1}, {5, 5, 5, 5}}));

        List<Matrix> listOfSubtables = matrixUnderTests.getSubtables();

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(new DenseMatrix(new double[][]
                {{3, 2}, {2, 4}, {4, 2}, {3, 1}, {2, 1}, {5, 5}}));

        MatrixAssert.assertThat(listOfSubtables.get(1)).isEqualTo(new DenseMatrix(new double[][]
                {{3, 2}, {3, 4}, {3, 2}, {2, 1}, {2, 1}, {5, 5}}));

        MatrixAssert.assertThat(listOfSubtables.get(2)).isEqualTo(new DenseMatrix(new double[][]
                {{4, 2}, {1, 4}, {5, 2}, {4, 1}, {4, 1}, {5, 5}}));

        MatrixAssert.assertThat(listOfSubtables.get(3)).isEqualTo(new DenseMatrix(new double[][]
                {{3, 2}, {2, 4}, {4, 2}, {3, 1}, {2, 1}, {5, 5}}));

        MatrixAssert.assertThat(listOfSubtables.get(4)).isEqualTo(new DenseMatrix(new double[][]
                {{3, 2}, {2, 4}, {4, 2}, {3, 1}, {2, 1}, {5, 5}}));


    }
}




