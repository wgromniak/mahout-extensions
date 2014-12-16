package org.mimuw.mahoutattrsel;

import org.mimuw.mahoutattrsel.api.RsesConverter;
import org.mimuw.mahoutattrsel.api.Subtable;
import rseslib.structure.table.DoubleDataTable;

/**
 * Neglects any info contained in the subtable apart from the actual data {@link org.apache.mahout.math.Matrix}.
 */
public final class RsesSubtableConverter implements RsesConverter<Subtable> {

    private static final RsesSubtableConverter INSTANCE = new RsesSubtableConverter();

    private RsesSubtableConverter() {}

    public static RsesSubtableConverter getInstance() {
        return INSTANCE;
    }

    @Override
    public DoubleDataTable convert(Subtable dataTable) {
        return RsesMatrixConverter.getInstance().convert(dataTable.getTable());
    }
}