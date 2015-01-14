package org.mimuw.mahoutattrsel.assertions;

import org.apache.mahout.math.Matrix;
import org.assertj.core.api.AbstractAssert;

/**
 * Assertion for the {@link org.apache.mahout.math.Matrix} type.
 */
public final class MatrixAssert extends AbstractAssert<MatrixAssert, Matrix> {

    protected MatrixAssert(Matrix actual) {
        super(actual, MatrixAssert.class);
    }

    /**
     * Verifies if the {@code actual} is equal to the {@code expected}. Matrices are equal iff they have the same
     * (equal) elements on the same places.
     *
     * @param expected the given Matrix to compare the actual value to.
     * @return {@code this} assertion object.
     */
    public MatrixAssert isEqualTo(Matrix expected) {

        if (actual.rowSize() != expected.rowSize() || actual.columnSize() != expected.columnSize()) {
            failWithMessage("The actual has different dimensions <%sx%s>, than the expected <%sx%s>",
                    actual.rowSize(), actual.columnSize(), expected.rowSize(), expected.columnSize());
        }

        for (int i = 0; i < expected.rowSize(); i++) {
            for (int j = 0; j < expected.columnSize(); j++) {

                if (actual.get(i, j) != expected.get(i, j)) {

                    failWithMessage("Expected <%s> to be equal to <%s>", expected, actual);
                }
            }
        }

        return myself;
    }

    /**
     * Verifies that the {@code actual} has given row size.
     *
     * @param expected expected row size.
     * @return {@code this} assertion object.
     */
    public MatrixAssert hasRowSize(int expected) {

        if (actual.rowSize() != expected) {
            failWithMessage("The actual row size <%s> is different than expected <%s>", actual.rowSize(), expected);
        }

        return myself;
    }

    /**
     * Verifies that the {@code actual} has given column size.
     *
     * @param expected expected column size.
     * @return {@code this} assertion object.
     */
    public MatrixAssert hasColumnSize(int expected) {

        if (actual.columnSize() != expected) {
            failWithMessage("The actual column size <%s> is different than expected <%s>",
                    actual.columnSize(), expected);
        }

        return myself;
    }
}
