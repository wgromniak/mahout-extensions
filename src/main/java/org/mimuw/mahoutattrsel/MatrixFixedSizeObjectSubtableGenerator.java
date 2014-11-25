package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.util.List;
import java.util.Random;

public final class MatrixFixedSizeObjectSubtableGenerator implements SubtableGenerator<Matrix> {

    private final Random random;

    public MatrixFixedSizeObjectSubtableGenerator(Random random) {
        this.random = random;
    }

    @Override
    public List<Matrix> getSubtables() {
        return null;
    }
}
