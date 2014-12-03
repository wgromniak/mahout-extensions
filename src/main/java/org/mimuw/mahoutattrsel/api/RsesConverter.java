package org.mimuw.mahoutattrsel.api;

import rseslib.structure.table.DoubleDataTable;

/**
 * Converts decision tables stored in given type {@code T} to rseslib's {@link DoubleDataTable}.
 *
 * <p>How the data are precisely converted depends on the implementation.
 *
 * @param <T> the type of the table that will be converted to {@link DoubleDataTable}.
 */
public interface RsesConverter<T> {

    /**
     * Converts given {@code dataTable} of type {@code T} to {@link DoubleDataTable}. It means that the output
     * contains the same information as the input table, i.e. the same objects with the same attribute and their values.
     *
     * @param dataTable data table of type {@code T}
     * @return converted table
     */
    DoubleDataTable convert(T dataTable);
}
