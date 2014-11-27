package org.mimuw.mahoutattrsel;

import com.google.common.collect.ImmutableList;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.util.BitSet;
import java.util.List;
import java.util.Random;

public class MatrixFixedSizeObjectSubtableGenerator implements SubtableGenerator<Matrix> {

    private final Random random;
    private final int numberOfSubtables;
    private final int subtableSize;

    private final Matrix dataTable;

    public MatrixFixedSizeObjectSubtableGenerator(Random random, int numberOfSubtables, int subtableSize, Matrix dataTable) {
        this.random = random;
        this.numberOfSubtables = numberOfSubtables;
        this.subtableSize = subtableSize;
        this.dataTable = dataTable;
    }

    public List<Matrix> getSubtables() {

        ImmutableList.Builder<Matrix> resultBuilder = ImmutableList.builder();

        int numberOfObjects = dataTable.columnSize();
        for (int i = 0; i < numberOfSubtables; i++) {

            BitSet selectedObjects = drawObjects(numberOfObjects, subtableSize);

            Matrix subtable = new DenseMatrix(subtableSize, dataTable.rowSize());

            int numOfRow = 0;

            for (int rowNum = 0; rowNum < numberOfObjects; rowNum++) {

                if (selectedObjects.get(rowNum)) {

                    subtable.assignRow(numOfRow, dataTable.viewRow(rowNum).clone());
                }
                numOfRow++;
            }

            resultBuilder.add(subtable);
        }

        return resultBuilder.build();
    }

    private  BitSet drawObjects(int numberOfObjects, int sizeOfSubtable) {

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
