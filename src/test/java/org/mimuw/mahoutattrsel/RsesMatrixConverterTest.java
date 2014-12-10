package org.mimuw.mahoutattrsel;

import com.google.common.collect.ImmutableMap;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.api.RsesConverter;
import org.testng.annotations.Test;
import rseslib.structure.attribute.Attribute;
import rseslib.structure.table.DoubleDataTable;

import static com.googlecode.catchexception.CatchException.catchException;
import static com.googlecode.catchexception.CatchException.caughtException;
import static org.assertj.core.api.Assertions.assertThat;

public class RsesMatrixConverterTest {

    @Test
    public void testConversionWithoutBindings() throws Exception {

        Matrix mat = new DenseMatrix(new double[][]{{1.2, 2, 3.14159265, 1}, {4, 5.31, 6, 0}, {7, 8, 9, 3}});

        RsesConverter<Matrix> converter =  RsesMatrixConverter.getInstance();

        DoubleDataTable doubleDataTable = converter.convert(mat);

        DoubleDataTableAssert.assertThat(doubleDataTable)
                .hasAttributeNames("attribute_0", "attribute_1", "attribute_2", "decision")
                .hasAttributeTypes(Attribute.Type.conditional, Attribute.Type.conditional, Attribute.Type.conditional,
                        Attribute.Type.decision)
                .hasAttributeValueSets(Attribute.ValueSet.numeric, Attribute.ValueSet.numeric,
                        Attribute.ValueSet.numeric, Attribute.ValueSet.nominal)
                .hasRow(0).whichContainsExactly(1.2, 2, 3.14159265, 1)
                .hasRow(1).whichContainsExactly(4, 5.31, 6, 0)
                .hasRow(2).whichContainsExactly(7, 8, 9, 3)
                .hasNoRow(3);
    }

    @Test
    public void testConversionFullBindings() throws Exception {

        Matrix mat = new DenseMatrix(new double[][]{{1.2, 2, 3.14159265, 1}, {4, 5.31, 6, 0}, {7, 8, 9, 3}});

        mat.setColumnLabelBindings(ImmutableMap.of("a0", 0, "a1", 1, "a2", 2, "d", 3));

        RsesConverter<Matrix> converter = RsesMatrixConverter.getInstance();

        DoubleDataTable doubleDataTable = converter.convert(mat);

        DoubleDataTableAssert.assertThat(doubleDataTable)
                .hasAttributeNames("a0", "a1", "a2", "d")
                .hasAttributeTypes(Attribute.Type.conditional, Attribute.Type.conditional, Attribute.Type.conditional,
                        Attribute.Type.decision)
                .hasAttributeValueSets(Attribute.ValueSet.numeric, Attribute.ValueSet.numeric,
                        Attribute.ValueSet.numeric, Attribute.ValueSet.nominal)
                .hasRow(0).whichContainsExactly(1.2, 2, 3.14159265, 1)
                .hasRow(1).whichContainsExactly(4, 5.31, 6, 0)
                .hasRow(2).whichContainsExactly(7, 8, 9, 3)
                .hasNoRow(3);
    }

    @Test
    public void testConversionPartialBindings() throws Exception {

        Matrix mat = new DenseMatrix(new double[][]{{1.2, 2, 3.14159265, 1}, {4, 5.31, 6, 0}, {7, 8, 9, 3}});

        mat.setColumnLabelBindings(ImmutableMap.of("a0", 0, "d", 3));

        RsesConverter<Matrix> converter =  RsesMatrixConverter.getInstance();

        DoubleDataTable doubleDataTable = converter.convert(mat);

        DoubleDataTableAssert.assertThat(doubleDataTable)
                .hasAttributeNames("a0", "attribute_1", "attribute_2", "d")
                .hasAttributeTypes(Attribute.Type.conditional, Attribute.Type.conditional, Attribute.Type.conditional,
                        Attribute.Type.decision)
                .hasAttributeValueSets(Attribute.ValueSet.numeric, Attribute.ValueSet.numeric,
                        Attribute.ValueSet.numeric, Attribute.ValueSet.nominal)
                .hasRow(0).whichContainsExactly(1.2, 2, 3.14159265, 1)
                .hasRow(1).whichContainsExactly(4, 5.31, 6, 0)
                .hasRow(2).whichContainsExactly(7, 8, 9, 3)
                .hasNoRow(3);
    }

    @Test
    public void testConversionDecisionOnly
            () throws Exception {

        Matrix mat = new DenseMatrix(new double[][]{{1}, {0}, {3}});

        RsesConverter<Matrix> converter =  RsesMatrixConverter.getInstance();

        DoubleDataTable doubleDataTable = converter.convert(mat);

        DoubleDataTableAssert.assertThat(doubleDataTable)
                .hasAttributeNames("decision")
                .hasAttributeTypes(Attribute.Type.decision)
                .hasAttributeValueSets(Attribute.ValueSet.nominal)
                .hasRow(0).whichContainsExactly(1)
                .hasRow(1).whichContainsExactly(0)
                .hasRow(2).whichContainsExactly(3)
                .hasNoRow(3);
    }

    @Test
    public void shouldThrowOnNegativeDecision() throws Exception {

        Matrix mat = new DenseMatrix(new double[][]{{1, -1}});

        RsesConverter<Matrix> converter =  RsesMatrixConverter.getInstance();

        catchException(converter).convert(mat);

        assertThat(caughtException())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Negative decision value: -1.0");
    }

    @Test
    public void shouldThrowOnNonIntegerDecision() throws Exception {

        Matrix mat = new DenseMatrix(new double[][]{{1, 3.14159265}});

        RsesConverter<Matrix> converter = RsesMatrixConverter.getInstance();

        catchException(converter).convert(mat);

        assertThat(caughtException())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Decision value is not an integer: 3.14159265");
    }
}