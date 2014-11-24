package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.util.List;

public final class MatrixFixedSizeObjectSubtableGenerator
        extends AbstractMatrixFixedSizeSubtableGenerator
        implements SubtableGenerator<Matrix> {

    @Override
    public List<Matrix> getSubtables() {
        return null;
    }
}
