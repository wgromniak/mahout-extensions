package org.mimuw.mahoutattrsel.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mimuw.mahoutattrsel.RsesSubtableConverter;
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

    public  static final String REDUCT_PROVIDER = "ReductProvider";
    public  static final String INDISCERNIBILITY_FOR_MISSING = "DiscernFromValue";
    public  static final String DISCERNIBILITY_METHOD = "OrdinaryDecisionAndInconsistenciesOmitted";
    public  static final String GeneralizedDecisionTransitiveClosure="TRUE";

    public static final String FIRST_PROPERTY = "IndiscernibilityForMissing";
    public static final String SECOMD_PROPERTY = "DiscernibilityMethod";
    public static final String THIRD_PROPERTY = "GeneralizedDecisionTransitiveClosure";

    @Override
    protected void map(IntWritable key, SubtableWritable value, Context context)
            throws IOException, InterruptedException {

        try {

            RsesSubtableConverter convertValue = new RsesSubtableConverter();

            Configuration conf = context.getConfiguration();

            ReductsProvider reductsProvider;

            Class<ReductsProvider> generatorClass = (Class<ReductsProvider>)
                    conf.getClass(REDUCT_PROVIDER, GlobalReductsProvider.class,ReductsProvider.class);

            Properties properties = new Properties();

            properties.setProperty(FIRST_PROPERTY, conf.get(FIRST_PROPERTY));
            properties.setProperty(SECOMD_PROPERTY, conf.get(SECOMD_PROPERTY));
            properties.setProperty(THIRD_PROPERTY, conf.get(THIRD_PROPERTY));

            reductsProvider = generatorClass.getConstructor(Properties.class, DoubleDataTable.class).
                    newInstance(properties, convertValue.convert(value.get()));

            int numberOfAllAttributes = value.get().getOriginalNumberOfAttributes();

            Collection<BitSet> reducts;

            reducts = reductsProvider.getReducts();

            for (BitSet actualReduct : reducts) {

                for (int numberOfActuallAttribute = 0; numberOfActuallAttribute < numberOfAllAttributes;
                     numberOfActuallAttribute++) {

                    addNewPair(value, context, actualReduct, numberOfActuallAttribute);
                }
            }
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalStateException("Error instantiating SubtableGenerator", e);
        }
    }

    private void addNewPair(SubtableWritable value, Context context, BitSet actualReduct, int numberOfActuallAttribute)
            throws IOException, InterruptedException {

        if (actualReduct.get(value.get().getAttributeAtPosition(numberOfActuallAttribute))) {

            System.out.println(value.get().getAttributeAtPosition(numberOfActuallAttribute) + " " + actualReduct);

            List<Integer> toIntWritableList = rewriteToList(actualReduct);

            IntListWritable toReturnListOfAttribute = new IntListWritable(toIntWritableList);

            IntWritable numberOfOriginalAttribute = new IntWritable(value.get().
                    getAttributeAtPosition(numberOfActuallAttribute));

            context.write(numberOfOriginalAttribute, toReturnListOfAttribute);
        }
    }

    private List<Integer> rewriteToList(BitSet actualReduct) {

        List<Integer> toIntWritableList = new ArrayList<>();

        for (int i = 0; i < actualReduct.size(); i++) {

            if (actualReduct.get(i)) {

                toIntWritableList.add(i);
            }
        }
        return toIntWritableList;
    }
}
