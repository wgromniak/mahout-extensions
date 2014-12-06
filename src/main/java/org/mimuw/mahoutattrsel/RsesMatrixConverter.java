package org.mimuw.mahoutattrsel;

import com.google.common.base.Optional;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.math.DoubleMath;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.Vector;
import org.mimuw.mahoutattrsel.api.RsesConverter;
import rseslib.structure.attribute.ArrayHeader;
import rseslib.structure.attribute.Attribute;
import rseslib.structure.attribute.Header;
import rseslib.structure.attribute.NominalAttribute;
import rseslib.structure.data.DoubleDataObject;
import rseslib.structure.table.ArrayListDoubleDataTable;
import rseslib.structure.table.DoubleDataTable;

import java.util.Map;

/**
 * {@inheritDoc}
 *
 * This implementation takes a data table in {@link Matrix} format and converts it to {@link ArrayListDoubleDataTable}.
 * The output data table contains an {@link ArrayHeader} with {@link Attribute}s inferred from the original table.
 * If the original table has column bindings the attributes are named after theses binding; if not, the attributes are
 * named "attribute_i" where i is the attribute's number and the decision is named "decision". The last column is
 * assumed to be a decision attribute. Rows are assumed to represent consecutive objects and are stored into
 * {@link DoubleDataObject}s.
 *
 * <p>This implementation does not allow missing values.
 */
public final class RsesMatrixConverter implements RsesConverter<Matrix> {

    @Override
    public DoubleDataTable convert(Matrix dataTable) {

        int numberOfAttributes = dataTable.columnSize();
        Attribute[] attributes = getAttributeArray(dataTable, numberOfAttributes);

        Header header = new ArrayHeader(attributes, null); // no missing values, thus null

        DoubleDataTable converted = new ArrayListDoubleDataTable(header);

        for (MatrixSlice row : dataTable) {

            DoubleDataObject dataObject = new DoubleDataObject(header);

            for (int j = 0; j < numberOfAttributes - 1; j++) {

                    dataObject.set(j, row.getQuick(j));
            }

            dataObject.setDecision(row.getQuick(numberOfAttributes - 1));

            converted.add(dataObject);
        }

        return converted;
    }

    private Attribute[] getAttributeArray(Matrix dataTable, int numberOfAttributes) {

        Map<String, Integer> colBindings = dataTable.getColumnLabelBindings();
        Optional<? extends BiMap<String, Integer>> colBindingsBiMap;

        if (colBindings == null) {
            colBindingsBiMap = Optional.absent();
        } else {
            colBindingsBiMap = Optional.of(ImmutableBiMap.copyOf(colBindings)); // TODO: will throw if inverse map is ambiguous
        }

        Attribute[] attributes = new Attribute[numberOfAttributes];

        for (int i = 0; i < numberOfAttributes - 1; i++) {

            Optional<String> colBinding;

            colBinding = getColBindingOpt(colBindingsBiMap, i);

            if (colBinding.isPresent()) {

                attributes[i] = new Attribute(Attribute.Type.conditional, Attribute.ValueSet.numeric, colBinding.get());

            } else {

                attributes[i] = new Attribute(Attribute.Type.conditional, Attribute.ValueSet.numeric, "attribute_" + i);
            }
        }

        Optional<String> decBinding;

        decBinding = getColBindingOpt(colBindingsBiMap, numberOfAttributes - 1);

        NominalAttribute decision;

        if (decBinding.isPresent()) {

            decision = new NominalAttribute(Attribute.Type.decision, decBinding.get());
        } else {

            decision = new NominalAttribute(Attribute.Type.decision, "decision");
        }

        int maxDecision = inferMaxDecision(dataTable);

        for (int i = 0; i <= maxDecision; i++) {

            decision.globalValueCode(String.valueOf(i)); // yes it's ugly
        }

        attributes[numberOfAttributes - 1] = decision;
        return attributes;
    }

    private Optional<String> getColBindingOpt(Optional<? extends BiMap<String, Integer>> colBindingsBiMap, int i) {

        Optional<String> colBinding;

        if (colBindingsBiMap.isPresent()) {

            String binding = colBindingsBiMap.get().inverse().get(i);

            if (binding != null) {

                colBinding = Optional.of(binding);

            } else {

                colBinding = Optional.absent();
            }
        } else {

            colBinding = Optional.absent();
        }

        return colBinding;
    }

    private int inferMaxDecision(Matrix dataTable) {

        int maxDecision = 0;

        Vector lastColumn = dataTable.viewColumn(dataTable.columnSize() - 1);

        for (int i = 0; i < lastColumn.size(); i++) {

            double currentDecision = lastColumn.getQuick(i);

            if (currentDecision < 0) {
                throw new IllegalArgumentException("Negative decision value: " + currentDecision);
            }

            if (!DoubleMath.isMathematicalInteger(currentDecision)) {
                throw new IllegalArgumentException("Decision value is not an integer: " + currentDecision);
            }

            if (currentDecision > maxDecision) {

                maxDecision = (int) currentDecision;
            }
        }

        return maxDecision;
    }
}
