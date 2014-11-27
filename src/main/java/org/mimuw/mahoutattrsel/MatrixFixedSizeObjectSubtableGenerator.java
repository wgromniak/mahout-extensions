package org.mimuw.mahoutattrsel;

import java.util.Random;

public class MatrixFixedSizeObjectSubtableGenerator extends  AbstractMatrixFixedSizeSubtableGenerator  {


    public MatrixFixedSizeObjectSubtableGenerator(Random random, int numberOfSubtables, int subtableSize,
                                                  double[][] dataTable) {

        super(random, numberOfSubtables,subtableSize,dataTable);
    }

}
