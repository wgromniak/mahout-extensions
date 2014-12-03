package org.mimuw.mahoutattrsel.api;

import org.apache.mahout.math.Matrix;

import java.util.List;

/**
 * This is an interface responsible for subtable generation.
 */
public interface SubtableGenerator<T> {

    public List<Matrix> getSubtables();

    //BitSet draw(int numberOfAttribute, int subtableSize);

}
