package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.util.BitSet;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MatrixFixedSizeAttributeSubtableGenerator implements SubtableGenerator<Matrix> {

    private  final Random random;
    private  final int numberOfSubtables;
    private  final int subTableSize;

    private final Matrix dataTable;


    public MatrixFixedSizeAttributeSubtableGenerator(Random random, int numberOfSubtables, int subTableSize, Matrix dataTable) {

        checkArgument(numberOfSubtables > 0);
        checkArgument(subTableSize > 0);

        this.random = checkNotNull(random);
        this.numberOfSubtables = numberOfSubtables;
        this.subTableSize = subTableSize;
        this.dataTable = checkNotNull(dataTable);
    }



    public List<Matrix> getSubtables() {


    checkArgument( dataTable.rowSize() == 2 );

        return null;
    }

    private  BitSet drawAttribute(int numberOfObjects, int sizeOfSubtable) {

        BitSet selected = new BitSet(numberOfObjects);

        while (sizeOfSubtable > 0) {

            int next = random.nextInt(numberOfObjects  );

            if (!selected.get(next)) {

                selected.set(next);
                sizeOfSubtable--;
            }
        }

        return selected;
    }
}
