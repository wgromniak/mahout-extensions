package org.mimuw.mahoutattrsel;

import com.google.common.collect.ImmutableList;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.util.BitSet;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractMatrixFixedSizeSubtableGenerator implements SubtableGenerator<Matrix> {

    protected Random random;
    protected  int numberOfSubtables;
    protected int subtableSize;

    protected  Matrix dataTable;

    public AbstractMatrixFixedSizeSubtableGenerator(Random random,  int numberOfSubtables, int subtableSize,
                                                      Matrix dataTable ) {


        checkArgument(numberOfSubtables > 0);
        checkArgument(subtableSize > 0);

        this.dataTable = checkNotNull(dataTable);
        this.subtableSize = subtableSize;
        this.numberOfSubtables = numberOfSubtables;
        this.random = checkNotNull(random);
    }

    public void chooseRows(ImmutableList.Builder<Matrix> resultBuilder, int numberOfObjects) {

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


