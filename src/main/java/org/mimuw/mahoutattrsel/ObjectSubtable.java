package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.Subtable;

import static com.google.common.base.Preconditions.checkNotNull;

public final class ObjectSubtable implements Subtable {

    private final Matrix table;

    public ObjectSubtable(Matrix table) {
        this.table = checkNotNull(table);
    }

    @Override
    public Matrix getTable() {
        return table;
    }

    @Override
    public int getAttributeAtPosition(int position) {
        return position;
    }

    @Override
    public boolean hasAllAttributes() {
        return true;
    }

    @Override
    public int getOriginalNumberOfAttributes() {
        return table.columnSize() - 1;
    }
}
