package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;

import static org.mockito.Mockito.mock;

public class MatrixFixedSizeObjectSubtableGeneratorTest {


    @Test
    public void dumyTestgetSubtables() throws Exception {
        MatrixFixedSizeObjectSubtableGenerator matrix = new MatrixFixedSizeObjectSubtableGenerator(new Random(), 1, 2,
                new DenseMatrix(new double[][]{{1, 2, 3},{7, 6 ,5}}));

        List<Matrix> listOfSubtables =  matrix.getSubtables();

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(new DenseMatrix(new double[][]{{1, 2 ,3}, {7, 6, 5}}));


    }


    @Test
    public void testSimple() throws Exception {

       MatrixFixedSizeObjectSubtableGenerator matrix = new MatrixFixedSizeObjectSubtableGenerator(new Random(), 2, 3,
                new DenseMatrix(new double[][]{{1, 2, 3},{7, 6 ,5}, {3, 3, 3}}));

        List<Matrix> listOfSubtables = matrix.getSubtables();

        MatrixAssert.assertThat(listOfSubtables.get(0)).isEqualTo(new DenseMatrix(new double[][]{{1, 2, 3},{7, 6 ,5}, {3, 3, 3}}));
        MatrixAssert.assertThat(listOfSubtables.get(1)).isEqualTo(new DenseMatrix(new double[][]{{1, 2, 3},{7, 6 ,5}, {3, 3, 3}}));

    }

    @Test
    public void testMockitogetSubtables() throws Exception {

        MatrixFixedSizeObjectSubtableGenerator test = mock(MatrixFixedSizeObjectSubtableGenerator.class);


    }
}
