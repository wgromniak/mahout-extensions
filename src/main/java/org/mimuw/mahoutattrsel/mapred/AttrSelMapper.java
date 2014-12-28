package org.mimuw.mahoutattrsel.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mimuw.mahoutattrsel.RsesSubtableConverter;
import org.mimuw.mahoutattrsel.api.Subtable;
import rseslib.processing.reducts.GlobalReductsProvider;
import rseslib.processing.reducts.ReductsProvider;
import rseslib.structure.table.DoubleDataTable;

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

    public static final String GRNRTSLIXEDECIISIONTRANSITIVECLOSURE =
            "mahout-extensions.attrsel.mahout-extensions.attrsel.DiscernibilityMethod";
    public static final String GRNRTSLIXEDECIISIONTRANSITIVECLOSURE_DEFAULT = "TRUE";

    public static final String JOHNSON_REDUCT = "mahout-extensions.attrsel.JohnsonReduct";
    public static final String JOHNSON_REDUCT_DEFAULT = "One";

    public static final String REDUCT_PROVIDER = "ReductProvider";

    @Override
    protected void map(IntWritable key, SubtableWritable value, Context context)
            throws IOException, InterruptedException {

        try {

            RsesSubtableConverter convertValue = new RsesSubtableConverter();

            Configuration conf = context.getConfiguration();

            ReductsProvider reductsProvider;

            @SuppressWarnings("unchecked")
            Class<ReductsProvider> generatorClass = (Class<ReductsProvider>)
                    conf.getClass(REDUCT_PROVIDER, GlobalReductsProvider.class, ReductsProvider.class);

            Properties properties = new Properties();

            properties.setProperty("IndiscernibilityForMissing",
                    conf.getTrimmed(INDISCERNIBILITY_FOR_MISSING, INDISCERNIBILITY_FOR_MISSING_DEFAULT));
            properties.setProperty("DiscernibilityMethod",
                    conf.getTrimmed(DISCERNIBILITY_METHOD, DISCERNIBILITY_METHOD_DEFAULT));
            properties.setProperty("GeneralizedDecisionTransitiveClosure",
                    conf.getTrimmed(
                            GRNRTSLIXEDECIISIONTRANSITIVECLOSURE, GRNRTSLIXEDECIISIONTRANSITIVECLOSURE_DEFAULT));
            properties.setProperty("JohnsonReduct",
                    conf.getTrimmed(
                            JOHNSON_REDUCT, JOHNSON_REDUCT_DEFAULT));

            reductsProvider = generatorClass.getConstructor(Properties.class, DoubleDataTable.class).
                    newInstance(properties, convertValue.convert(value.get()));

            Subtable subtable = value.get();

            Collection<BitSet> reducts;

            reducts = reductsProvider.getReducts();

            for (BitSet actualReduct : reducts) {

                List<Integer> listReduct = new ArrayList<>();

                for (int i = actualReduct.nextSetBit(0); i >= 0; i = actualReduct.nextSetBit(i + 1)) {

                    listReduct.add(subtable.getAttributeAtPosition(i));
                }

                for (int attribute : listReduct) {

                    context.write(new IntWritable(attribute), new IntListWritable(listReduct));
                }
            }

        } catch (InstantiationException | IllegalAccessException |
                InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalStateException("Error instantiating ReductsProvider");
        }
    }
}
