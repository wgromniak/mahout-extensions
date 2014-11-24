package org.mimuw.mahoutattrsel;

import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.util.List;
import java.util.Random;

public abstract class AbstractMatrixFixedSizeSubtableGenerator implements SubtableGenerator<Matrix> {

    private final Random random = RandomUtils.getRandom(); // they provide good Random

    @Override
    abstract public List<Matrix> getSubtables();
}
