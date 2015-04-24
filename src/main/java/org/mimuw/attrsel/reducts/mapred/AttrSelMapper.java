package org.mimuw.attrsel.reducts.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mimuw.attrsel.common.SubtableWritable;
import org.mimuw.attrsel.reducts.RandomReducts;
import org.mimuw.attrsel.reducts.RsesDiscretizer;
import rseslib.processing.discretization.ChiMergeDiscretizationProvider;
import rseslib.processing.reducts.JohnsonReductsProvider;
import rseslib.processing.reducts.ReductsProvider;
import rseslib.system.PropertyConfigurationException;

import java.io.IOException;
import java.util.List;

/**
 * Input - ( no of subtable, subtable ), output - ( attribute, reduct ).
 */
final class AttrSelMapper extends Mapper<IntWritable, SubtableWritable, IntWritable, IntListWritable> {

    @Override
    protected void map(IntWritable key, SubtableWritable value, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        @SuppressWarnings("unchecked")
        Class<ReductsProvider> reductsProviderClass = (Class<ReductsProvider>)
                conf.getClass("ReductsProvider", JohnsonReductsProvider.class, ReductsProvider.class);

        RandomReducts randomReducts;

        if (!conf.getBoolean("noDiscretize", false)) {
            try {
                randomReducts =
                        new RandomReducts(
                                value.get(),
                                reductsProviderClass,
                                new RsesDiscretizer(
                                        new ChiMergeDiscretizationProvider(
                                                conf.getInt("numDiscIntervals", 4),
                                                conf.getDouble("discSignificance", 0.25)
                                        )
                                ),
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
            } catch (PropertyConfigurationException e) {
                throw new IllegalStateException(e);
            }
        } else {
            randomReducts =
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
        }

        for (List<Integer> reduct: randomReducts.getReducts()) {
            for (int attr : reduct) {
                context.write(new IntWritable(attr), new IntListWritable(reduct));
            }
        }
    }
}
