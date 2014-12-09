package org.mimuw.mahoutattrsel;

import com.google.common.collect.ImmutableList;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.Subtable;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AttributeSubtableTest {

    @Test
    public void testSubtable() throws Exception {

        Matrix matrix = new DenseMatrix(new double[][]{{1, 2, 3}, {4, 5, 6}, {6, 7, 8}});

        Subtable subtable = new AttributeSubtable(matrix, ImmutableList.of(3, 8), 10);

        assertThat(subtable.getOriginalNumberOfAttributes()).isEqualTo(10);
        assertThat(subtable.getNumberOfAttributes()).isEqualTo(2);
        assertThat(subtable.hasAllAttributes()).isFalse();
        assertThat(subtable.iterateAttributes()).containsExactly(3, 8);
        MatrixAssert.assertThat(subtable.getTable()).isEqualTo(matrix);
    }
}