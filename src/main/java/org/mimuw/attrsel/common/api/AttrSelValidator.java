package org.mimuw.attrsel.common.api;

import org.apache.mahout.math.Matrix;

import java.util.List;

/**
 * An interface representing facility used to validate an attribute selection experiment.
 *
 * @param <T> type of the returned metric describing quality of selected attributes
 */
public interface AttrSelValidator<T> {

    /**
     * Validates the selected attributes on the given data table.
     *
     * @param dataTable data that should be used to validate selected attributes (usually the original data table)
     * @param selectedAttributes list of the numbers of selected attributes
     * @return the metric describing quality of the attributes
     */
    T validate(Matrix dataTable, List<Integer> selectedAttributes);
}
