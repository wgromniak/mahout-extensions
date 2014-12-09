package org.mimuw.mahoutattrsel;

import com.google.common.collect.UnmodifiableIterator;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.Subtable;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public final class ObjectSubtable implements Subtable {

    private final Matrix table;

    /**
     * Creates an attribute subtable, based on given {@link Matrix} table. The table is not copied internally! Any
     * changes made to it are visible in this subtable.
     */
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
    public int getNumberOfAttributes() {
        return table.columnSize() - 1; // - decision
    }

    @Override
    public Iterable<Integer> iterateAttributes() {
        return new Iterable<Integer>() {
            @Override
            public Iterator<Integer> iterator() {
                return new UnmodifiableIterator<Integer>() {

                    private int current = 0;

                    @Override
                    public boolean hasNext() {
                        return current < table.columnSize() - 1; // - decision
                    }

                    @Override
                    public Integer next() {
                        return current++;
                    }
                };
            }
        };
    }

    @Override
    public int getOriginalNumberOfAttributes() {
        return table.columnSize() - 1;
    }
}
