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
 * This implementation generate (numberOfsubtables) subtables and each of them  have got (subtableSize + 1 ) columns
 * where the last column is "decision" column and rows as many as input matrix from data table matrix.
 * Number of columns which are chose are randomly generated but rows are sorted in subtables (Tu put it simply when we
 * draw lots 3,5,1 then  subtable will have first column - 1, second column - 2,and third - 5).
 */
public final class MatrixFixedSizeAttributeSubtableGenerator extends AbstractMatrixFixedSizeSubtableGenerator
        implements SubtableGenerator<Matrix> {

    public MatrixFixedSizeAttributeSubtableGenerator(Random random, int numberOfSubtables, int subtableSize,
                                                     Matrix dataTable) {

        super(random, numberOfSubtables, subtableSize, dataTable);
        checkArgument(subtableSize < dataTable.columnSize());
    }

    @Override
    public List<Matrix> getSubtables() {

        Matrix dataTableTranspose = dataTable.transpose();

        ImmutableList.Builder<Matrix> resultBuilder = ImmutableList.builder();

        int numberOfAttributes = dataTableTranspose.rowSize() - 1;

        for (int i = 0; i < numberOfSubtables; i++) {

            BitSet selectedObjects = draw(numberOfAttributes, subtableSize);

            Matrix subtable = new DenseMatrix(subtableSize + 1, dataTableTranspose.columnSize());

            int numOfRow = 0;

            for (int rowNum = 0; rowNum < numberOfAttributes; rowNum++) {

                if (selectedObjects.get(rowNum)) {

                    subtable.assignRow(numOfRow, dataTableTranspose.viewRow(rowNum).clone());
                    numOfRow++;
                }
            }

            subtable.assignRow(numOfRow, dataTableTranspose.viewRow(dataTableTranspose.rowSize() - 1).clone());

            subtable = subtable.transpose();

            resultBuilder.add(subtable);

        }

        return resultBuilder.build();
    }

}
