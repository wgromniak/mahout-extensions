package org.mimuw.attrsel.trees;

import gov.sandia.cognition.learning.algorithm.tree.AbstractDecisionTreeNode;
import gov.sandia.cognition.learning.algorithm.tree.CategorizationTreeLearner;
import gov.sandia.cognition.learning.algorithm.tree.CategorizationTreeNode;
import gov.sandia.cognition.learning.algorithm.tree.DeciderLearner;
import gov.sandia.cognition.learning.data.InputOutputPair;
import gov.sandia.cognition.learning.function.categorization.Categorizer;
import gov.sandia.cognition.statistics.distribution.DefaultDataDistribution;

import java.util.Collection;
import java.util.Map;

class CategorizationTreeLearnerStoringCardinality<InputType, OutputType>
        extends CategorizationTreeLearner<InputType, OutputType> {

    public CategorizationTreeLearnerStoringCardinality() {
    }

    public CategorizationTreeLearnerStoringCardinality(
            DeciderLearner<? super InputType, OutputType, ?, ?> deciderLearner) {
        super(deciderLearner);
    }

    public CategorizationTreeLearnerStoringCardinality(
            DeciderLearner<? super InputType, OutputType, ?, ?> deciderLearner,
            int leafCountThreshold,
            int maxDepth) {
        super(deciderLearner, leafCountThreshold, maxDepth);
    }

    public CategorizationTreeLearnerStoringCardinality(
            DeciderLearner<? super InputType, OutputType, ?, ?> deciderLearner,
            int leafCountThreshold,
            int maxDepth,
            Map<OutputType, Double> priors) {
        super(deciderLearner, leafCountThreshold, maxDepth, priors);
    }

    @Override
    protected CategorizationTreeNode<InputType, OutputType, ?> learnNode(
            Collection<? extends InputOutputPair<? extends InputType, OutputType>> data,
            AbstractDecisionTreeNode<InputType, OutputType, ?> parent) {

        if (data == null || data.size() <= 0)
        {
            // Invalid data, nothing to learn. This case should never happen.
            return null;
        }

        // We put the most probable output category on every node in
        // the tree, in case we get a bad decision function or leaf
        // node. This ensures That we can always make a
        // categorization.
        OutputType mostProbOutput = computeMaxProbPrediction(data);

        // Give the node we are creating the most probable output.
        final CategorizationTreeNode<InputType, OutputType, Object> node =
                new CategorizationTreeNodeWithCardinality<>(parent, mostProbOutput, data.size()); // our node

        // Check for termination conditions that produce a leaf node.
        boolean isLeaf = this.areAllOutputsEqual(data)
                || data.size() <= this.leafCountThreshold
                || (this.maxDepth > 0 && node.getDepth() >= this.maxDepth);

        if (!isLeaf)
        {
            // Search for a decision function to split the data.
            final Categorizer<? super InputType, ? extends Object> decider =
                    this.getDeciderLearner().learn(data);

            if (decider != null)
            {
                // We learned a good decider.
                node.setDecider(decider);

                // Learn the child nodes.
                super.learnChildNodes(node, data, decider);
            }
            else
            {
                // Failed to find a decision function ==> there is no
                // attribute that separates the values of different
                // output categories.  This node necessarily becomes a
                // leaf.
                isLeaf = true;
            }
        }

        // Return the new node we've created.
        return node;
    }

    /**
     * Return the most probable output value, taking into
     * consideration both the frequency counts in the data sample (at
     * the current node) and the prior proabalities for possible
     * output values.
     *
     * @param data
     *    The data sample, with output labels for each data point.
     *    The sample must contain at least 1 data point.
     * @return The output value with highest conditional probability.
     */
    private OutputType computeMaxProbPrediction(
            final Collection<? extends InputOutputPair<?, OutputType>> data)
    {
        DefaultDataDistribution<OutputType> nodeCounts = getOutputCounts(data);
        if (priors == null) {
            // With no explicit prior, the most probable prediction is
            // the most common category.
            return nodeCounts.getMaxValueKey();
        }

        // Iterate over possible predictions, and keep track of the
        // prediction with highest probability.  Note that these
        // probabilities are not normalized.  (It would be easy, just
        // divide by sum of the unnormalized probs . . . but since
        // that would be a constant scaling, the maximum probability
        // prediction would be the same.)
        double bestProb = -1.0;
        OutputType bestVal = null;
        for (OutputType category : nodeCounts.getDomain()) {
            double likelihood =
                    nodeCounts.get(category) / trainCounts.get(category);
            double prior = priors.get(category);
            double prob = prior * likelihood;
            if (prob > bestProb) {
                bestProb = prob;
                bestVal = category;
            }
        }

        return bestVal;
    }
}
