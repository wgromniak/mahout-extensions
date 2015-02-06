package org.mimuw.mahoutattrsel.api;

import java.util.List;
/**
 * This is an interface responsible for subtable generation.
 */
public interface SubtableGenerator<T> {

    /**
     * Returned subtables may be views on the origins table, according to the implementation.
     *
     * @return unmodifiable list of subtables
     */
    List<T> getSubtables();

    /**
     * Returns a list whose i-th entry contains the number of subtables the i-th attribute occurs in.
     * (decision excluded)
     *
     * @return unmodifiable list with counts of subtables per attribute
     */
    List<Integer> getNumberOfSubtablesPerAttribute();
}
