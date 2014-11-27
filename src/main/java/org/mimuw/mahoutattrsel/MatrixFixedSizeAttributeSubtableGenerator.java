package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;

import java.util.Random;

public final class MatrixFixedSizeAttributeSubtableGenerator extends AbstractMatrixFixedSizeSubtableGenerator {


    public MatrixFixedSizeAttributeSubtableGenerator(Random random, int numberOfSubtables, int subtableSize,
                                                     Matrix dataTable) {

        super(random,numberOfSubtables, numberOfSubtables, dataTable);
        dataTable.transpose();
        dataTable.rowSize() -= 1;


    }

    private Matrix eraseDecisionColumnAndTransposition(Matrix matrix) {

        Matrix withOutDecision = new DenseMatrix(matrix.rowSize()-1,matrix.columnSize());

        matrix = matrix.transpose();

        for(int numOfrow = 0; numOfrow < matrix.columnSize() - 1; numOfrow++) {

            withOutDecision = matrix.assignRow(numOfrow,matrix.viewRow(numOfrow).clone());
        }

        return  withOutDecision;
    }
}
