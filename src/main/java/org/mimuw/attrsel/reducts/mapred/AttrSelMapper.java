package org.mimuw.attrsel.reducts.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mimuw.attrsel.common.SubtableWritable;
import org.mimuw.attrsel.reducts.RandomReducts;
import rseslib.processing.reducts.JohnsonReductsProvider;
import rseslib.processing.reducts.ReductsProvider;

import java.io.IOException;
import java.util.List;

/**
 * Input - ( no of subtable, subtable ), output - ( attribute, reduct ).
 */
final class AttrSelMapper extends Mapper<IntWritable, SubtableWritable, IntWritable, IntListWritable> {

    public static final String REDUCTS_PROVIDER = "ReductsProvider";

    @Override
    protected void map(IntWritable key, SubtableWritable value, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        @SuppressWarnings("unchecked")
        Class<ReductsProvider> reductsProviderClass = (Class<ReductsProvider>)
                conf.getClass(REDUCTS_PROVIDER, JohnsonReductsProvider.class, ReductsProvider.class);

        RandomReducts randomReducts =
                new RandomReducts(
                        value.get(),
                        reductsProviderClass,
                        conf.getEnum(
                                RandomReducts.IndiscernibilityForMissing.class.getSimpleName(),
                                RandomReducts.IndiscernibilityForMissing.DiscernFromValue
                        ),
                        conf.getEnum(
                                RandomReducts.DiscernibilityMethod.class.getSimpleName(),
                                RandomReducts.DiscernibilityMethod.OrdinaryDecisionAndInconsistenciesOmitted
                        ),
                        conf.getEnum(
                                RandomReducts.GeneralizedDecisionTransitiveClosure.class.getSimpleName(),
                                RandomReducts.GeneralizedDecisionTransitiveClosure.TRUE
                        ),
                        conf.getEnum(
                                RandomReducts.JohnsonReducts.class.getSimpleName(),
                                RandomReducts.JohnsonReducts.All
                        )
                );

        for (List<Integer> reduct: randomReducts.getReducts()) {
            for (int attr : reduct) {
                context.write(new IntWritable(attr), new IntListWritable(reduct));
            }
        }
    }
}
