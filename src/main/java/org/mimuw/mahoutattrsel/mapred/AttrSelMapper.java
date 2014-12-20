package org.mimuw.mahoutattrsel.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mimuw.mahoutattrsel.RsesSubtableConverter;
import rseslib.processing.reducts.GlobalReductsProvider;
import rseslib.processing.reducts.ReductsProvider;
import rseslib.system.PropertyConfigurationException;

import java.io.IOException;
import java.util.*;

/**
 * Input - ( no of subtable, subtable ), output - ( attribute, reduct ).
 */
public final class AttrSelMapper extends Mapper<IntWritable, SubtableWritable, IntWritable, IntListWritable> {

    @Override
    protected void map(IntWritable key, SubtableWritable value, Context context)
            throws IOException, InterruptedException {

        try {

            Properties props = new Properties();
            props.setProperty("IndiscernibilityForMissing", "DiscernFromValue");
            props.setProperty("DiscernibilityMethod", "OrdinaryDecisionAndInconsistenciesOmitted");
            props.setProperty("GeneralizedDecisionTransitiveClosure", "TRUE");

            RsesSubtableConverter convertValue = new RsesSubtableConverter();

            int numberOfAllAttributes = value.get().getOriginalNumberOfAttributes();

            ReductsProvider reductsProvider = new GlobalReductsProvider(props, convertValue.convert(value.get()));

            Collection<BitSet> reducts;

            reducts = reductsProvider.getReducts();

            for (BitSet actualReduct : reducts) {

                for (int numberOfActualReduct = 0; numberOfActualReduct < numberOfAllAttributes;
                     numberOfActualReduct++) {

                    addNewPair(value, context, actualReduct, numberOfActualReduct);
                }
            }

        } catch (PropertyConfigurationException e) {
            throw new IllegalStateException();
        }
    }

    private void addNewPair(SubtableWritable value, Context context, BitSet actualReduct, int numberOfActualReduct)
            throws IOException, InterruptedException {

        if (actualReduct.get(numberOfActualReduct)) {

            List<Integer> toIntWritableList = rewriteToList(actualReduct);

            IntListWritable toReturnListOfAttribute = new IntListWritable(toIntWritableList);

            IntWritable numberOfOriginalAttribute = new IntWritable(value.get().
                    getAttributeAtPosition(numberOfActualReduct));

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
