package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.Subtable;
import org.testng.annotations.Test;

import static com.googlecode.catchexception.CatchException.catchException;
import static com.googlecode.catchexception.CatchException.caughtException;
import static org.mimuw.mahoutattrsel.assertions.AttrselAssertions.assertThat;

public class ObjectSubtableTest {

    @Test
    public void testSubtable() throws Exception {

        Matrix matrix = new DenseMatrix(new double[][]{{1, 2, 3}, {4, 5, 6}, {6, 7, 8}});

        Subtable subtable = new ObjectSubtable(matrix);

        assertThat(subtable.getOriginalNumberOfAttributes()).isEqualTo(2);
        assertThat(subtable.getNumberOfAttributes()).isEqualTo(2);
        assertThat(subtable.hasAllAttributes()).isTrue();
        assertThat(subtable.getAttributeAtPosition(1)).isEqualTo(1);
        assertThat(subtable.iterateAttributes()).containsExactly(0, 1);
        assertThat(subtable.getTable()).isEqualTo(matrix);

        catchException(subtable).getAttributeAtPosition(2);
        assertThat(caughtException()).isInstanceOf(IndexOutOfBoundsException.class);
    }
}