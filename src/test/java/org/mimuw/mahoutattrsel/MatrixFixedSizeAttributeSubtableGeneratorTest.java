package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;

public class MatrixFixedSizeAttributeSubtableGeneratorTest {

    @Test
    public void testName() throws Exception {

        MatrixFixedSizeAttributeSubtableGenerator matrix = new MatrixFixedSizeAttributeSubtableGenerator(new Random(), 1, 2,
                new DenseMatrix(new double[][]{{1,5},{2,6},{3,7},{4,8}} ));

        List<Matrix> listOfMatrix = matrix.getSubtables();

     //   MatrixAssert.assertThat(listOfMatrix.get(0)).isEqualTo( new DenseMatrix(new double[][]{{1,2},{6 ,5}}) );


    }
}




