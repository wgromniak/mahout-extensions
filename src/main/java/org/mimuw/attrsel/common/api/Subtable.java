package org.mimuw.attrsel.common.api;

import org.apache.mahout.math.Matrix;

/**
 * Everywhere in this interface, it is assumed that the original table (this subtable comes from) has attributes
 * numbered {@code 0, 1, 2, ..., n}, for some {@code n}. Decision is not included as an attribute.
 */
public interface Subtable {

    /**
     * Returns a modifiable view on the actual data in this subtable. Any changes made to the returned table will
     * change this subtable.
     *
     * @return matrix containing the actual data in this subtable
     */
    Matrix getTable();

    /**
     * Returns the number of the attribute (from the original table this subtable was created from) at given position,
     * i.e. corresponding to the column of the table retured by {@link Subtable#getTable()} at this position.
     *
     * @throws IndexOutOfBoundsException if this subtable doesn't have that many attributes
     */
    int getAttributeAtPosition(int position);

    /**
     * Whether this subtable has all the attributes from the original table.
     *
     * @return true iff this table has the same attributes as the original table
     */
    boolean hasAllAttributes();

    /**
     * @return number of attributes in this subtable
     */
    int getNumberOfAttributes();

    /**
     * @return an {@link Iterable} over the attributes in this subtable
     */
    Iterable<Integer> iterateAttributes();

    /**
     * @return the number of attributes the original table had (decision excluded)
     */
    int getOriginalNumberOfAttributes();
}
