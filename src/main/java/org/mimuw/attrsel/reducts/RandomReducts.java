package org.mimuw.attrsel.reducts;

import org.mimuw.attrsel.common.api.Subtable;
import rseslib.processing.reducts.ReductsProvider;
import rseslib.structure.table.DoubleDataTable;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

public class RandomReducts {

    public enum IndiscernibilityForMissing { DiscernFromValue, DiscernFromValueOneWay, DontDiscernFromValue }

    public enum DiscernibilityMethod {
        All,
        OrdinaryDecisionAndInconsistenciesOmitted,
        GeneralizedDecision,
        GeneralizedDecisionAndOrdinaryChecked
    }

    public enum GeneralizedDecisionTransitiveClosure { TRUE, FALSE }

    public enum JohnsonReducts { All, One }

    private final RsesSubtableConverter converter = RsesSubtableConverter.getInstance();
    private final Subtable subtable;
    private final ReductsProvider reductsProvider;

    public RandomReducts(
            Subtable subtable,
            Class<? extends ReductsProvider> reductsProviderClass,
            IndiscernibilityForMissing indiscernibilityForMissing,
            DiscernibilityMethod discernibilityMethod,
            GeneralizedDecisionTransitiveClosure generalizedDecisionTransitiveClosure,
            JohnsonReducts johnsonReducts) {

        Properties props = new Properties();
        props.setProperty(IndiscernibilityForMissing.class.getSimpleName(), indiscernibilityForMissing.toString());
        props.setProperty(DiscernibilityMethod.class.getSimpleName(), discernibilityMethod.toString());
        props.setProperty(
                GeneralizedDecisionTransitiveClosure.class.getSimpleName(),
                generalizedDecisionTransitiveClosure.toString()
        );
        props.setProperty(JohnsonReducts.class.getSimpleName(), johnsonReducts.toString());

        this.subtable = checkNotNull(subtable);

        try {
            this.reductsProvider = reductsProviderClass
                    .getConstructor(Properties.class, DoubleDataTable.class)
                    .newInstance(props, converter.convert(subtable));
        } catch (InstantiationException | IllegalAccessException |
                InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalStateException("Error instantiating ReductsProvider", e);
        }
    }

    public RandomReducts(
            Subtable subtable,
            Class<? extends ReductsProvider> reductsProviderClass,
            RsesDiscretizer discretizer,
            IndiscernibilityForMissing indiscernibilityForMissing,
            DiscernibilityMethod discernibilityMethod,
            GeneralizedDecisionTransitiveClosure generalizedDecisionTransitiveClosure,
            JohnsonReducts johnsonReducts) {

        Properties props = new Properties();
        props.setProperty(IndiscernibilityForMissing.class.getSimpleName(), indiscernibilityForMissing.toString());
        props.setProperty(DiscernibilityMethod.class.getSimpleName(), discernibilityMethod.toString());
        props.setProperty(
                GeneralizedDecisionTransitiveClosure.class.getSimpleName(),
                generalizedDecisionTransitiveClosure.toString()
        );
        props.setProperty(JohnsonReducts.class.getSimpleName(), johnsonReducts.toString());

        this.subtable = checkNotNull(subtable);

        try {
            this.reductsProvider = reductsProviderClass
                    .getConstructor(Properties.class, DoubleDataTable.class)
                    .newInstance(props, discretizer.discretize(converter.convert(subtable)));
        } catch (InstantiationException | IllegalAccessException |
                InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalStateException("Error instantiating ReductsProvider", e);
        }
    }

    public List<List<Integer>> getReducts() {

        Collection<BitSet> reducts = reductsProvider.getReducts();

        List<List<Integer>> result = new ArrayList<>(reducts.size());

        for (BitSet actualReduct : reducts) {

            List<Integer> listReduct = new ArrayList<>(actualReduct.cardinality());

            for (int i = actualReduct.nextSetBit(0); i >= 0; i = actualReduct.nextSetBit(i + 1)) {

                listReduct.add(subtable.getAttributeAtPosition(i)); // TODO: this throws when decision is in the reduct - investigate
            }

            result.add(listReduct);
        }

        return result;
    }
}
