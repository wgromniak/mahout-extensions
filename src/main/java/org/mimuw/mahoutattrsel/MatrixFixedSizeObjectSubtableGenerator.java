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

public class MatrixFixedSizeObjectSubtableGenerator implements SubtableGenerator<Matrix> {

    private final Random random;
    private final int numberOfSubtables;
    private final int subtableSize;

    private final Matrix dataTable;

    public MatrixFixedSizeObjectSubtableGenerator(Random random, int numberOfSubtables, int subtableSize,
                                                  Matrix dataTable) throws IllegalArgumentException {

        checkArgument(numberOfSubtables > 0);
        checkArgument(subtableSize > 0);

        this.random = checkNotNull(random);
        this.numberOfSubtables = numberOfSubtables;
        this.subtableSize = subtableSize;
        this.dataTable = checkNotNull(dataTable);


        if (subtableSize > dataTable.rowSize()) {

            throw new IllegalArgumentException();
        }
    }

    public List<Matrix> getSubtables() {

        ImmutableList.Builder<Matrix> resultBuilder = ImmutableList.builder();

        int numberOfObjects = dataTable.rowSize();

        for (int i = 0; i < numberOfSubtables; i++) {

            BitSet selectedObjects = drawObjects(numberOfObjects, subtableSize);

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


    private BitSet drawObjects(int numberOfObjects, int sizeOfSubtable) {

        BitSet selected = new BitSet(numberOfObjects);

        while (sizeOfSubtable > 0) {

            int next = random.nextInt(numberOfObjects);

            if (!selected.get(next)) {

                selected.set(next);
                sizeOfSubtable--;
            }
        }

        return selected;
    }
}
