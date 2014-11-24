package org.mimuw.mahoutattrsel.api;

import org.apache.mahout.math.Matrix;
import rseslib.structure.table.DoubleDataTable;

/**
 * Converts decision tables stored in given type T to rseslib DoubleDataTable.
 */
public interface RsesConverter<T> {

    DoubleDataTable convert(T decisionTable);
}
