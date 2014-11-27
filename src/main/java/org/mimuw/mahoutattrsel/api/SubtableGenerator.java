package org.mimuw.mahoutattrsel.api;

import com.google.common.collect.ImmutableList;
import org.apache.mahout.math.Matrix;

/**
 * This is an interface responsible for subtable generation.
 */
public interface SubtableGenerator<T> {

    public void chooseRows(ImmutableList.Builder<Matrix> resultBuilder, int numberOfObjects);

}
