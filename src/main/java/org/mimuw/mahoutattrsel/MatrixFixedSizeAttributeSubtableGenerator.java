package org.mimuw.mahoutattrsel;

import com.google.common.collect.ImmutableList;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.util.BitSet;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

public final class MatrixFixedSizeAttributeSubtableGenerator extends AbstractMatrixFixedSizeSubtableGenerator
        implements SubtableGenerator<Matrix> {

    public MatrixFixedSizeAttributeSubtableGenerator(Random random, int numberOfSubtables, int subtableSize,
                                                     Matrix dataTable) {

        super(random, numberOfSubtables, subtableSize, dataTable);
        checkArgument(subtableSize < dataTable.columnSize());
    }

    public List<Matrix> getSubtables() {

        Matrix dataTableTranspose = dataTable.transpose();

        ImmutableList.Builder<Matrix> resultBuilder = ImmutableList.builder();

        int numberOfAttributes = dataTableTranspose.rowSize() - 1;

        for (int i = 0; i < numberOfSubtables; i++) {

            BitSet selectedObjects = draw(numberOfAttributes, subtableSize);

            Matrix subtable = new DenseMatrix(subtableSize + 1, dataTableTranspose.columnSize());

            int numOfRow = 0;

            for (int rowNum = 0; rowNum < numberOfAttributes; rowNum++) {

                if (selectedObjects.get(rowNum)) {

                    subtable.assignRow(numOfRow, dataTableTranspose.viewRow(rowNum).clone());
                    numOfRow++;
                }
            }

            subtable.assignRow(numOfRow, dataTableTranspose.viewRow(dataTableTranspose.rowSize() - 1).clone());

            subtable = subtable.transpose();

            resultBuilder.add(subtable);

        }

        return resultBuilder.build();
    }

}
