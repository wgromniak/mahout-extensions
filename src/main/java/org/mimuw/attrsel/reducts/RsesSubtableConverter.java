package org.mimuw.attrsel.reducts;

import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.reducts.api.RsesConverter;
import rseslib.structure.table.DoubleDataTable;

import static com.google.common.base.Preconditions.checkNotNull;

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
        checkNotNull(dataTable, "Expected Subtable not to be  null");
        return RsesMatrixConverter.getInstance().convert(dataTable.getTable());
    }
}
