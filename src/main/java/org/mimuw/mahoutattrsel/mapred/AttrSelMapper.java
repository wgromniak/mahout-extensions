package org.mimuw.mahoutattrsel.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mimuw.mahoutattrsel.RsesDiscretizer;
import org.mimuw.mahoutattrsel.RsesSubtableConverter;
import org.mimuw.mahoutattrsel.api.Subtable;
import rseslib.processing.discretization.ChiMergeDiscretizationProvider;
import rseslib.processing.reducts.JohnsonReductsProvider;
import rseslib.processing.reducts.ReductsProvider;
import rseslib.structure.table.DoubleDataTable;
import rseslib.system.PropertyConfigurationException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Input - ( no of subtable, subtable ), output - ( attribute, reduct ).
 */
public final class AttrSelMapper extends Mapper<IntWritable, SubtableWritable, IntWritable, IntListWritable> {

    public static final String INDISCERNIBILITY_FOR_MISSING = "mahout-extensions.attrsel.IndiscernibilityForMissing";
    public static final String INDISCERNIBILITY_FOR_MISSING_DEFAULT = "DiscernFromValue";

    public static final String DISCERNIBILITY_METHOD = "mahout-extensions.attrsel.DiscernibilityMethod";
    public static final String DISCERNIBILITY_METHOD_DEFAULT = "OrdinaryDecisionAndInconsistenciesOmitted";

    public static final String GENERALIZED_DECISION_TRANSITIVE_CLOSURE =
            "mahout-extensions.attrsel.mahout-extensions.attrsel.DiscernibilityMethod";
    public static final String GENERALIZED_DECISION_TRANSITIVE_CLOSURE_DEFAULT = "TRUE";

    public static final String JOHNSON_REDUCTS = "mahout-extensions.attrsel.JohnsonReducts";
    public static final String JOHNSON_REDUCTS_DEFAULT = "All";

    public static final String REDUCT_PROVIDER = "ReductProvider";


    private final RsesSubtableConverter converter = RsesSubtableConverter.getInstance();
    private RsesDiscretizer discretizer;
    private Properties reductsProviderProperties;

    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        try {
            discretizer = new RsesDiscretizer(new ChiMergeDiscretizationProvider(4, 0.5)); // TODO: hardcoded temporarily
        } catch (PropertyConfigurationException e) {
            throw new IllegalStateException("Cannot happen");
        }

        Configuration conf = context.getConfiguration();

        reductsProviderProperties = new Properties();

        reductsProviderProperties.setProperty("IndiscernibilityForMissing",
                conf.getTrimmed(INDISCERNIBILITY_FOR_MISSING, INDISCERNIBILITY_FOR_MISSING_DEFAULT));
        reductsProviderProperties.setProperty("DiscernibilityMethod",
                conf.getTrimmed(DISCERNIBILITY_METHOD, DISCERNIBILITY_METHOD_DEFAULT));
        reductsProviderProperties.setProperty("GeneralizedDecisionTransitiveClosure",
                conf.getTrimmed(
                        GENERALIZED_DECISION_TRANSITIVE_CLOSURE, GENERALIZED_DECISION_TRANSITIVE_CLOSURE_DEFAULT));
        reductsProviderProperties.setProperty("JohnsonReducts",
                conf.getTrimmed(JOHNSON_REDUCTS, JOHNSON_REDUCTS_DEFAULT));
    }

    @Override
    protected void map(IntWritable key, SubtableWritable value, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        Subtable subtable = value.get();
        DoubleDataTable table = converter.convert(subtable);

        if (conf.getBoolean("attrsel.Discretize", true)) {

            table = discretizer.discretize(table);
        }

        ReductsProvider reductsProvider = getReductsProvider(conf, table);

        Collection<BitSet> reducts = reductsProvider.getReducts();

        for (BitSet actualReduct : reducts) {

            List<Integer> listReduct = new ArrayList<>();

            for (int i = actualReduct.nextSetBit(0); i >= 0; i = actualReduct.nextSetBit(i + 1)) {

                listReduct.add(subtable.getAttributeAtPosition(i)); // TODO: this throws when decision is in the reduct - investigate
            }

            for (int attribute : listReduct) {

                context.write(new IntWritable(attribute), new IntListWritable(listReduct));
            }
        }
    }

    private ReductsProvider getReductsProvider(Configuration conf, DoubleDataTable table) {
        try {
            @SuppressWarnings("unchecked")
            Class<ReductsProvider> generatorClass = (Class<ReductsProvider>)
                    conf.getClass(REDUCT_PROVIDER, JohnsonReductsProvider.class, ReductsProvider.class);

            return generatorClass.getConstructor(Properties.class, DoubleDataTable.class).
                    newInstance(reductsProviderProperties, table);

        } catch (InstantiationException | IllegalAccessException |
                 InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalStateException("Error instantiating ReductsProvider", e);
        }
    }
}
