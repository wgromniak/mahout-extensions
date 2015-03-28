package org.mimuw.attrsel.trees;

import gov.sandia.cognition.collection.CollectionUtil;
import gov.sandia.cognition.learning.algorithm.tree.CategorizationTreeLearner;
import gov.sandia.cognition.learning.algorithm.tree.VectorThresholdInformationGainLearner;
import gov.sandia.cognition.learning.data.InputOutputPair;
import gov.sandia.cognition.learning.function.categorization.VectorElementThresholdCategorizer;
import gov.sandia.cognition.math.matrix.Vectorizable;
import gov.sandia.cognition.statistics.distribution.DefaultDataDistribution;
import gov.sandia.cognition.util.DefaultPair;
import gov.sandia.cognition.util.DefaultWeightedValue;

import java.util.ArrayList;
import java.util.Collection;

public class VectorThresholdInformationGainLearnerStoringGain<OutputType>
        extends VectorThresholdInformationGainLearner<OutputType> {

    /**
     * This is almost a copy and paste from AbstractVectorThresholdMaximumGainLearner. Only the return value is changed.
     */
    @Override
    public VectorElementThresholdCategorizer learn(
            final Collection<? extends InputOutputPair<? extends Vectorizable, OutputType>> data) {
        final int totalCount = CollectionUtil.size(data);
        if (totalCount <= 1) {
            // Nothing to learn.
            return null;
        }

        // Compute the base count values for the node.
        final DefaultDataDistribution<OutputType> baseCounts =
                CategorizationTreeLearner.getOutputCounts(data);

        // Pre-allocate a workspace of data for computing the gain.
        final ArrayList<DefaultWeightedValue<OutputType>> workspace =
                new ArrayList<>(totalCount);
        for (int i = 0; i < totalCount; i++) {
            workspace.add(new DefaultWeightedValue<OutputType>());
        }

        // Figure out the dimensionality of the data.
        final int dimensionality = getDimensionality(data);

        // Go through all the dimensions to find the one with the best gain
        // and the best threshold.
        double bestGain = -1.0;
        int bestIndex = -1;
        double bestThreshold = 0.0;

        final int dimensionsCount = this.dimensionsToConsider == null ?
                dimensionality : this.dimensionsToConsider.length;
        for (int i = 0; i < dimensionsCount; i++) {
            final int index = this.dimensionsToConsider == null ?
                    i : this.dimensionsToConsider[i];

            // Compute the best gain-threshold pair for the given dimension of
            // the data.
            final DefaultPair<Double, Double> gainThresholdPair =
                    this.computeBestGainAndThreshold(data, index, baseCounts);

            if (gainThresholdPair == null) {
                // There was no gain-threshold pair that created a
                // threshold.
                continue;
            }

            // Get the gain from the pair.
            final double gain = gainThresholdPair.getFirst();

            // Determine if this is the best gain seen.
            if (bestIndex == -1 || gain > bestGain) {
                // This is the best gain, so store the gain, threshold,
                // and index.
                final double threshold = gainThresholdPair.getSecond();
                bestGain = gain;
                bestIndex = index;
                bestThreshold = threshold;
            }
        }

        if (bestIndex < 0) {
            // There was no dimension that provided any gain for the data,
            // so no decision function can be made.
            return null;
        } else {
            // Create the decision function for the best gain.
            return new VectorElementThresholdCategorizerWithGain(
                    bestIndex,
                    bestThreshold,
                    bestGain
            );
        }
    }
}
