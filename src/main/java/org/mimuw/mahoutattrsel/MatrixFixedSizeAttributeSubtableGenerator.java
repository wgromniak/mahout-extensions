package org.mimuw.mahoutattrsel;

import com.google.common.collect.ImmutableList;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.util.BitSet;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class MatrixFixedSizeAttributeSubtableGenerator implements SubtableGenerator<Matrix> {

    private final Random random;
    private final int numberOfSubtables;
    private final int subtableSize;

    private final Matrix dataTable;


    public MatrixFixedSizeAttributeSubtableGenerator(Random random, int numberOfSubtables, int subtableSize,
                                                     Matrix dataTable) {

        checkArgument(numberOfSubtables > 0);
        checkArgument(subtableSize > 0);
        checkArgument(subtableSize < dataTable.columnSize());

        this.random = checkNotNull(random);
        this.numberOfSubtables = numberOfSubtables;
        this.subtableSize = subtableSize;
        this.dataTable = checkNotNull(dataTable);

    }

    public List<Matrix> getSubtables() {

        Matrix dataTableTranspose = dataTable.transpose();

        ImmutableList.Builder<Matrix> resultBuilder = ImmutableList.builder();

        int numberOfAttributes = dataTableTranspose.rowSize() - 1;

        for (int i = 0; i < numberOfSubtables; i++) {

            BitSet selectedObjects = drawAttribute(numberOfAttributes, subtableSize);

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

    private BitSet drawAttribute(int numberOfAttribute, int sizeOfSubtable) {

        BitSet selected = new BitSet(numberOfAttribute);

        while (sizeOfSubtable > 0) {

            int next = random.nextInt(numberOfAttribute);

            if (!selected.get(next)) {

                selected.set(next);
                sizeOfSubtable--;
            }
        }

        return selected;
    }
}
