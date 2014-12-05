package org.mimuw.mahoutattrsel;

import com.google.common.collect.ImmutableList;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.util.BitSet;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @inheritDoc This implementation takes four parameters, random, number of subtables(integer) , size of
 * subtables (integer),data table (Matrix) and generate subtables of Matrix.This implementation returns List of Matrix.
 * This implementation generate (numberOfsubtables) subtables and each of them  have got exactly (subtableSize) rows
 * from data table matrix. Number of rows which are chose are randomly generated but rows are sorted in subtables
 * (Tu put it simply when we draw lots 3,5,1 then  subtable will have first row - 1, second row - 2, and third - 5)
 */
public final class MatrixFixedSizeObjectSubtableGenerator extends AbstractMatrixFixedSizeSubtableGenerator
        implements SubtableGenerator<Matrix> {


    public MatrixFixedSizeObjectSubtableGenerator(Random random, int numberOfSubtables, int subtableSize,
                                                  Matrix dataTable) {

        super(random, numberOfSubtables, subtableSize, dataTable);
        checkArgument(subtableSize <= dataTable.rowSize());

    }

    @Override
    public List<Matrix> getSubtables() {

        ImmutableList.Builder<Matrix> resultBuilder = ImmutableList.builder();

        int numberOfObjects = dataTable.rowSize();

        for (int i = 0; i < numberOfSubtables; i++) {

            BitSet selectedObjects = draw(numberOfObjects, subtableSize);

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
}
