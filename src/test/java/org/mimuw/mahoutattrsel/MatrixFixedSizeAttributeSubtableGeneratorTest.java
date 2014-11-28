package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;

public class MatrixFixedSizeAttributeSubtableGeneratorTest {

    @Test
    public void testName() throws Exception {

        MatrixFixedSizeAttributeSubtableGenerator matrix = new MatrixFixedSizeAttributeSubtableGenerator(new Random(), 1, 3,
                new DenseMatrix(new double[][]{{1,2,3,4}, {5,6,7,8}} ));

        List<Matrix> listOfMatrix = matrix.getSubtables();

        MatrixAssert.assertThat(listOfMatrix.get(0)).isEqualTo( new DenseMatrix(new double[][]{{1,2,3}, {5,6,7}}) );


    }
}




