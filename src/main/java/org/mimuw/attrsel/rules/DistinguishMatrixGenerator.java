package org.mimuw.attrsel.rules;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;

import java.util.*;

import static com.google.gson.internal.$Gson$Preconditions.checkNotNull;


public class DistinguishMatrixGenerator {

    Matrix inputDataMatirx;

    public DistinguishMatrixGenerator(Matrix inputDataMatirx) {

        checkNotNull(inputDataMatirx);
        this.inputDataMatirx = inputDataMatirx;
    }


    /*
     1) Computing distinguish matrix for all table
     2) Get one column of this distinguish matrix
     */
    @SuppressWarnings("unchecked")
    public Set<Integer>[][] computeDistinguishMatrix() {
        int sizie = this.inputDataMatirx.rowSize();
        Set[][] costam = new Set[sizie][sizie];
        for (int i = 0; i < sizie; i++) {
            for (int j = 0; j < sizie; j++) {
                costam[i][j] = new HashSet<>();
            }
        }

        for (MatrixSlice firstObject : this.inputDataMatirx) {
            for (MatrixSlice secondObject : this.inputDataMatirx)
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
                        costam[secondObject.index()][firstObject.index()].add(put);
                        costam[firstObject.index()][secondObject.index()].add(put);
                    }
                }
        }
        return costam;
    }
}
