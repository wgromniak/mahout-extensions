package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.Subtable;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.util.BitSet;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

abstract class AbstractMatrixFixedSizeSubtableGenerator implements SubtableGenerator<Subtable> {

    final Random random;
    final int numberOfSubtables;
    final int subtableSize;

    final Matrix dataTable;

    List<Subtable> subtables;
    List<Integer> numberOfSubtablesPerAttribute;

    AbstractMatrixFixedSizeSubtableGenerator(Random random, int numberOfSubtables, int subtableSize, Matrix dataTable) {

        checkArgument(numberOfSubtables > 0);
        checkArgument(subtableSize > 0);

        this.random = checkNotNull(random);
        this.numberOfSubtables = numberOfSubtables;
        this.subtableSize = subtableSize;
        this.dataTable = checkNotNull(dataTable);
    }

    @Override
    public final List<Subtable> getSubtables() {
        if (subtables == null) {
            calculateSubtables();
        }
        return subtables;
    }

    @Override
    public final List<Integer> getNumberOfSubtablesPerAttribute() {
        if (numberOfSubtablesPerAttribute == null) {
            calculateSubtables();
        }
        return numberOfSubtablesPerAttribute;
    }

    /**
     * This method should must initialise numberOfSubtablesPerAttribute and numberOfSubtablesPerAttribute.
     */
    abstract void calculateSubtables();

    final BitSet draw(int numberOfSamples, int subtableSize) {

        BitSet selected = new BitSet(subtableSize);

        int numberOfIteration = numberOfSamples - subtableSize;

        if (numberOfSamples < 2 * subtableSize) {

            while (numberOfIteration > 0) {

                int next = random.nextInt(numberOfSamples);

                if (!selected.get(next)) {

                    selected.set(next);
                    numberOfIteration--;
                }
            }

            selected.flip(0, numberOfSamples);

            return selected;

        } else {

            while (subtableSize > 0) {

                int next = random.nextInt(numberOfSamples);

                if (!selected.get(next)) {

                    selected.set(next);
                    subtableSize--;
                }
            }

            return selected;
        }
    }

}
