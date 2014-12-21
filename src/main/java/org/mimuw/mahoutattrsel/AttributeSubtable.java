package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.Subtable;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class  AttributeSubtable implements Subtable {

    private final Matrix table;
    private final List<Integer> attributes;
    private final int originalNumberOfAttributes;

    /**
     * Creates an attribute subtable, based on given {@link Matrix} table and with given attributes. The table and
     * list of attributes are not copied internally! Any changes made to them will be visible in this subtable.
     */
    public AttributeSubtable(Matrix table, List<Integer> attributes, int originalNumberOfAttributes) {
        checkNotNull(table, "Expected matrix not to be null");
        checkNotNull(attributes, "Expected attribute list not to be null");
        checkArgument(table.columnSize() - 1 == attributes.size(), "Attributes do not correspond to the table");
        checkArgument(attributes.size() <= originalNumberOfAttributes, "More attributes than their original number");

        this.table = table;             // reuse, since it may be large
        this.attributes = attributes;   // -,,-
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
        return originalNumberOfAttributes == attributes.size();
    }

    @Override
    public int getNumberOfAttributes() {
        return attributes.size();
    }

    @Override
    public Iterable<Integer> iterateAttributes() {
        return new Iterable<Integer>() {
            @Override
            public Iterator<Integer> iterator() {
                return attributes.iterator();
            }
        };
    }

    @Override
    public int getOriginalNumberOfAttributes() {
        return originalNumberOfAttributes;
    }
}
