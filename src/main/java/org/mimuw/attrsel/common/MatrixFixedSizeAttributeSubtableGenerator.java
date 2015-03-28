package org.mimuw.attrsel.common;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * {@inheritDoc}
 * <p/>
 * This implementation takes four parameters, random, number of subtables(integer) , size of
 * subtables (integer),data table (Matrix) and generate subtables of Matrix.This implementation returns List of Matrix.
 * This implementation generate (numberOfsubtables) subtables and each of them  have got (subtableSize + 1 ) columns
 * where the last column is "decision" column and rows as many as input matrix from data table matrix.
 * Number of columns which are chose are randomly generated but rows are sorted in subtables (Tu put it simply when we
 * draw lots 3,5,1 then  subtable will have first column - 1, second column - 2,and third - 5).
 *
 * <p>The results returned via getSubtables() and getNumberOfSubtablesPerAttribute() are initialized lazily, i.e.
 * when either of this methods is called.
 */
public final class MatrixFixedSizeAttributeSubtableGenerator extends AbstractMatrixFixedSizeSubtableGenerator
        implements SubtableGenerator<Subtable> {

    public MatrixFixedSizeAttributeSubtableGenerator(Random random, int numberOfSubtables, int subtableSize,
                                                     Matrix dataTable) {

        super(random, numberOfSubtables, subtableSize, dataTable);
        checkArgument(subtableSize < dataTable.columnSize());
    }

    @Override
    void calculateSubtables() {

        ImmutableList.Builder<Subtable> resultBuilder = ImmutableList.builder();

        int numberOfAttributes = dataTable.columnSize() - 1;

        int[] numberOfSubtablesPerAttribute = new int[numberOfAttributes]; // array since it's already inited to 0s

        for (int i = 0; i < numberOfSubtables; i++) {

            BitSet selectedObjects = draw(numberOfAttributes, subtableSize);

            Matrix subtable = new DenseMatrix(subtableSize + 1, dataTable.rowSize());

            List<Integer> attributes = new ArrayList<>();

            int numOfRow = 0;

            for (int rowNum = 0; rowNum < numberOfAttributes; rowNum++) {

                if (selectedObjects.get(rowNum)) {

                    subtable.assignRow(numOfRow, dataTable.viewColumn(rowNum));
                    attributes.add(rowNum);
                    numOfRow++;

                    numberOfSubtablesPerAttribute[rowNum]++;
                }
            }

            subtable.assignRow(numOfRow, dataTable.viewColumn(dataTable.columnSize() - 1));

            subtable = subtable.transpose();

            Subtable toReturn = new AttributeSubtable(subtable, attributes, dataTable.columnSize() - 1);

            resultBuilder.add(toReturn);

        }

        this.subtables = resultBuilder.build();
        this.numberOfSubtablesPerAttribute = Ints.asList(numberOfSubtablesPerAttribute);
    }

}
