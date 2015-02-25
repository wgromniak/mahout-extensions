package org.mimuw.mahoutattrsel;

import org.mimuw.mahoutattrsel.api.Discretizer;
import rseslib.processing.discretization.AbstractDiscretizationProvider;
import rseslib.processing.transformation.Transformer;
import rseslib.structure.data.DoubleData;
import rseslib.structure.table.ArrayListDoubleDataTable;
import rseslib.structure.table.DoubleDataTable;

import static com.google.common.base.Preconditions.checkNotNull;

public final class RsesDiscretizer implements Discretizer<DoubleDataTable> {

    private final AbstractDiscretizationProvider discretizationProvider;

    public RsesDiscretizer(AbstractDiscretizationProvider discretizationProvider) {
        this.discretizationProvider = checkNotNull(discretizationProvider);
    }

    @Override
    public DoubleDataTable discretize(DoubleDataTable dataTable) {

        Transformer transformer = discretizationProvider.generateTransformer(dataTable);

        DoubleDataTable result = new ArrayListDoubleDataTable(dataTable.attributes());

        for (DoubleData object : dataTable.getDataObjects()) {
            result.add(transformer.transformToNew(object));
        }

        return result;
    }
}
