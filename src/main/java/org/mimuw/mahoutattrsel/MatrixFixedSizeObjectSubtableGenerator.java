package org.mimuw.mahoutattrsel;

import com.google.common.collect.ImmutableList;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.util.BitSet;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

public final class MatrixFixedSizeObjectSubtableGenerator extends AbstractSubtableGenerator implements SubtableGenerator<Matrix> {


    public MatrixFixedSizeObjectSubtableGenerator(Random random, int numberOfSubtables, int subtableSize,
                                                  Matrix dataTable) {

        super(random, numberOfSubtables, subtableSize, dataTable);
        checkArgument(subtableSize <= dataTable.rowSize());

    }

    public List<Matrix> getSubtables() {

        ImmutableList.Builder<Matrix> resultBuilder = ImmutableList.builder();

        int numberOfObjects = dataTable.rowSize();

        for (int i = 0; i < numberOfSubtables; i++) {

            BitSet selectedObjects = draw(numberOfObjects, subtableSize);

            Matrix subtable = new DenseMatrix(subtableSize, dataTable.columnSize());

            int numOfRow = 0;

            for (int rowNum = 0; rowNum < numberOfObjects; rowNum++) {

                if (selectedObjects.get(rowNum)) {

                    subtable.assignRow(numOfRow, dataTable.viewRow(rowNum).clone());
                    numOfRow++;
                }
            }

            resultBuilder.add(subtable);
        }

        return resultBuilder.build();
    }
}
