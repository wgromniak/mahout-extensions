package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.Matrix;

import java.util.Random;

public final class MatrixFixedSizeAttributeSubtableGenerator extends AbstractMatrixFixedSizeSubtableGenerator {


    public MatrixFixedSizeAttributeSubtableGenerator(Random random, int numberOfSubtables, int subtableSize, Matrix dataTable) {
        super(random, numberOfSubtables, subtableSize, dataTable);
    }

}
