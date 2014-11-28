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

public class MatrixFixedSizeAttributeSubtableGenerator implements SubtableGenerator<Matrix> {

    private final Random random;
    private final int numberOfSubtables;
    private final int subTableSize;

    private Matrix dataTable;       //canot be final, because we have to transpose matrix


    public MatrixFixedSizeAttributeSubtableGenerator(Random random, int numberOfSubtables, int subTableSize, Matrix dataTable) {

        checkArgument(numberOfSubtables > 0);
        checkArgument(subTableSize > 0);

        this.random = checkNotNull(random);
        this.numberOfSubtables = numberOfSubtables;
        this.subTableSize = subTableSize;
        this.dataTable = checkNotNull(dataTable);
    }


    public List<Matrix> getSubtables() {


        dataTable = dataTable.transpose().clone();

        ImmutableList.Builder<Matrix> resultBuilder = ImmutableList.builder();

        int numberOfAttributes = dataTable.rowSize();
        numberOfAttributes--;

        for (int i = 0; i < numberOfAttributes; i++) {

            BitSet selectedObjects = drawAttribute(numberOfAttributes, subTableSize);

            Matrix subtable = new DenseMatrix(subTableSize, dataTable.columnSize());

            int numOfRow = 0;

            for (int rowNum = 0; rowNum < numberOfAttributes; rowNum++) {

                if (selectedObjects.get(rowNum)) {

                    subtable.assignRow(numOfRow, dataTable.viewRow(rowNum).clone());
                    numOfRow++;
                }
            }

            subtable = subtable.transpose().clone();

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
