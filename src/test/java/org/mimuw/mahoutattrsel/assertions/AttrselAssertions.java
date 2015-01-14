package org.mimuw.mahoutattrsel.assertions;

import org.apache.mahout.math.Matrix;
import rseslib.structure.table.DoubleDataTable;

public final class AttrselAssertions extends org.assertj.core.api.Assertions {

    public static DoubleDataTableAssert assertThat(DoubleDataTable actual) {
        return new DoubleDataTableAssert(actual);
    }

    public static MatrixAssert assertThat(Matrix actual) {
        return new MatrixAssert(actual);
    }
}
