package org.mimuw.attrsel.rules;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public class DistinguishMatrixGenerator {

    private final Matrix inputDataMatrix;
    private Set<Integer>[][] distinguishTable;
    private final int numberOfObjects;

    @SuppressWarnings("unchecked")
    public DistinguishMatrixGenerator(Matrix inputDataMatirx) {

        checkNotNull(inputDataMatirx);
        checkArgument(inputDataMatirx.rowSize() > 0);
        checkArgument(inputDataMatirx.columnSize() > 0);
        this.inputDataMatrix = inputDataMatirx;
        this.numberOfObjects = this.inputDataMatrix.rowSize();
        this.distinguishTable = new Set[numberOfObjects][numberOfObjects];
    }

    /*
     1) Computing distinguish matrix for all table
     2) Get one column of this distinguish matrix
     */

    public Set<Integer>[][] computeDistinguishMatrix() {
        for (int i = 0; i < numberOfObjects; i++) {
            for (int j = 0; j < numberOfObjects; j++) {
                distinguishTable[i][j] = new HashSet<>();
            }
        }

        for (MatrixSlice firstObject : this.inputDataMatrix) {
            for (MatrixSlice secondObject : this.inputDataMatrix)
                if ((firstObject.get(firstObject.size() - 1)) != (secondObject.get(secondObject.size() - 1))) {
                    int count = firstObject.size() - 2;//I don't want decision
                    ArrayList<Integer> put = new ArrayList<>();
                    while (count >= 0) {
                        if (firstObject.get(count) != secondObject.get(count)) {
                            put.add(count);
                        }
                        count--;
                    }
                    if (!put.isEmpty()) {
                        Collections.reverse(put);
                        this.distinguishTable[secondObject.index()][firstObject.index()].addAll(put);
                        this.distinguishTable[firstObject.index()][secondObject.index()].addAll(put);
                    }
                }
        }
        return distinguishTable;
    }

    public ArrayList<Set> getColumn(int numberOfColumn) {
        ArrayList<Set> expectedColumn  = new ArrayList<>();
        for (int i = 0; i < this.numberOfObjects; i++) {
                expectedColumn.add(this.distinguishTable[i][numberOfColumn]);
        }
        return expectedColumn;
    }
}
