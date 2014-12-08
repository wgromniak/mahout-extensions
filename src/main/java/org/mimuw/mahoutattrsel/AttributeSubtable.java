package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.Subtable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class AttributeSubtable implements Subtable {

    private final Matrix table;
    private final List<Integer> attributes;
    private final int originalNumberOfAttributes;

    public AttributeSubtable(Matrix table, List<Integer> attributes, int originalNumberOfAttributes) {
        checkNotNull(table);
        checkNotNull(attributes);
        checkArgument(table.columnSize() - 1 == attributes.size());
        checkArgument(attributes.size() >= originalNumberOfAttributes);

        this.table = table;
        this.attributes = attributes;
        this.originalNumberOfAttributes = originalNumberOfAttributes;
    }

    @Override
    public Matrix getTable() {
        return table;
    }

    @Override
    public int getAttributeAtPosition(int position) {
        return attributes.get(position);
    }

    @Override
    public boolean hasAllAttributes() {
        return false;
    }

    @Override
    public int getOriginalNumberOfAttributes() {
        return originalNumberOfAttributes;
    }
}
