package org.mimuw.attrsel.assertions;

import org.assertj.core.api.AbstractAssert;
import rseslib.structure.attribute.Attribute;
import rseslib.structure.data.DoubleData;
import rseslib.structure.table.DoubleDataTable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Assertion for the {@link DoubleDataTable} type.
 */
public final class DoubleDataTableAssert extends AbstractAssert<DoubleDataTableAssert, DoubleDataTable> {

    protected DoubleDataTableAssert(DoubleDataTable actual) {
        super(actual, DoubleDataTableAssert.class);
    }

    public DoubleDataTableAssert hasAttributeNames(String... expected) {

        if (actual.attributes().noOfAttr() != expected.length) {
            failWithMessage("Actual has different number of attributes <%s> than expected <%s>",
                    actual.attributes().noOfAttr(), expected.length);
        }

        for (int i = 0; i < expected.length; i++) {

            if (!actual.attributes().name(i).equals(expected[i])) {
                failWithMessage("Expected attribute <%s> at position <%s>, found <%s>",
                        expected[i], i, actual.attributes().name(i));
            }
        }

        return myself;
    }

    public DoubleDataTableAssert hasAttributeTypes(Attribute.Type... expected) {

        if (actual.attributes().noOfAttr() != expected.length) {
            failWithMessage("Actual has different number of attributes <%s> than expected <%s>",
                    actual.attributes().noOfAttr(), expected.length);
        }

        for (int i = 0; i < expected.length; i++) {

            boolean licit;

            switch (expected[i]) {
                case conditional:
                    licit = actual.attributes().isConditional(i);
                    break;
                case decision:
                    licit = actual.attributes().isDecision(i);
                    break;
                case text:
                    licit = actual.attributes().isText(i);
                    break;
                default:
                    licit = false;
                    break;
            }

            if (!licit) {
                failWithMessage("Expected attribute at position <%s> to be of type <%s>, but was not", i, expected[i]);
            }
        }

        return myself;
    }

    public DoubleDataTableAssert hasAttributeValueSets(Attribute.ValueSet... expected) {

        if (actual.attributes().noOfAttr() != expected.length) {
            failWithMessage("Actual has different number of attributes <%s> than expected <%s>",
                    actual.attributes().noOfAttr(), expected.length);
        }

        for (int i = 0; i < expected.length; i++) {

            boolean isLicit;

            switch (expected[i]) {
                case nominal:
                    isLicit = actual.attributes().isNominal(i);
                    break;
                case numeric:
                    isLicit = actual.attributes().isNumeric(i);
                    break;
                default:
                    isLicit = false;
                    break;
            }

            if (!isLicit) {
                failWithMessage("Expected attribute at position <%s> to have value set <%s>, but it didn't",
                        i, expected[i]);
            }
        }

        return myself;
    }

    public RowAssert hasRow(int index) {

        try {
            actual.getDataObjects().get(index);
        } catch (IndexOutOfBoundsException e) {
            failWithMessage("Expected the table to have row at index <%s>, but it didn't", index);
        }

        return new RowAssert(actual.getDataObjects().get(index));
    }

    public DoubleDataTableAssert hasNoRow(int index) {
        try {
            actual.getDataObjects().get(index);
            // this is unexpected
            failWithMessage("Expected the table not to have row at index <%s>, but it did", index);
        } catch (IndexOutOfBoundsException e) {
            // this is expected
        }

        return myself;
    }

    public final class RowAssert {

        private final DoubleData actual;

        private RowAssert(DoubleData actual) {
            this.actual = checkNotNull(actual);
        }

        public DoubleDataTableAssert whichContainsExactly(double... expected) {

            int i;

            for (i = 0; i < expected.length; i++) {

                try {
                    if (Double.compare(expected[i], actual.get(i)) != 0) {

                        failWithMessage("Row element at index <%s> was expected to be <%s>, but was <%s>",
                                i, expected[i], actual.get(i));
                    }
                } catch (ArrayIndexOutOfBoundsException e) {

                    failWithMessage("Expected <%s> at index <%s>, but there wasn't", expected[i], i);
                }
            }

            try {
                actual.get(i + 1);
                // this is unexpected
                failWithMessage("Expected no element at index <%s>, but was <%s>", i + 1, actual.get(i + 1));

            } catch (ArrayIndexOutOfBoundsException e) {
                // this is expected
            }

            return myself;
        }
    }
}
