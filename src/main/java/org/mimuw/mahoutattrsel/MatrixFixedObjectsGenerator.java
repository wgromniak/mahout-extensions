package org.mimuw.mahoutattrsel;


import com.google.common.collect.ImmutableList;
import org.apache.mahout.math.Matrix;

import java.util.List;
import java.util.Random;

public abstract class MatrixFixedObjectsGenerator extends  AbstractMatrixFixedSizeSubtableGenerator {

    public MatrixFixedObjectsGenerator(Random random, int numberOfSubtables, int subtableSize, Matrix dataTableRandom) {
        super(random, numberOfSubtables, subtableSize, dataTableRandom);
    }

    public List<Matrix> getSubtable() {

        ImmutableList.Builder<Matrix> resultBuilder = ImmutableList.builder();

        int numberOfObjects = dataTable.columnSize();

        choose(resultBuilder, numberOfObjects);

        return resultBuilder.build();
    }

}
