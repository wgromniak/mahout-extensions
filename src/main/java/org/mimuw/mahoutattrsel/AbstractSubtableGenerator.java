package org.mimuw.mahoutattrsel;


import org.apache.mahout.math.Matrix;

import java.util.BitSet;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

abstract class AbstractSubtableGenerator {

    final Random random;
    final int numberOfSubtables;
    final int subtableSize;

    final Matrix dataTable;

    protected AbstractSubtableGenerator(Random random, int numberOfSubtables, int subtableSize, Matrix dataTable) {

        checkArgument(numberOfSubtables > 0);
        checkArgument(subtableSize > 0);

        this.random = checkNotNull(random);
        this.numberOfSubtables = numberOfSubtables;
        this.subtableSize = subtableSize;
        this.dataTable = checkNotNull(dataTable);

    }

    BitSet draw(int numberOfAttribute, int sizeOfSubtable) {

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
