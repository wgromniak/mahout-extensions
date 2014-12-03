package org.mimuw.mahoutattrsel;


import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.AbstractMatrix;

import java.util.BitSet;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

abstract class AbstractMatrixFixedSizeSubtableGenerator implements AbstractMatrix {

    final Random random;
    final int numberOfSubtables;
    final int subtableSize;

    final Matrix dataTable;

    AbstractMatrixFixedSizeSubtableGenerator(Random random, int numberOfSubtables, int subtableSize, Matrix dataTable) {

        checkArgument(numberOfSubtables > 0);
        checkArgument(subtableSize > 0);

        this.random = checkNotNull(random);
        this.numberOfSubtables = numberOfSubtables;
        this.subtableSize = subtableSize;
        this.dataTable = checkNotNull(dataTable);

    }

    public BitSet draw(int numberOfSamples, int sizeOfSubtable) {

        BitSet selected = new BitSet(numberOfSamples);
        int sizeOfSubtableConst = sizeOfSubtable;

        while (sizeOfSubtable > 0) {

            int next = random.nextInt(numberOfSamples);

            if (!selected.get(next)) {

                selected.set(next);
                sizeOfSubtable--;
            }
        }

        return selected;
    }
}
