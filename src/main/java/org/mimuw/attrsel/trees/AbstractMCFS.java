package org.mimuw.attrsel.trees;

import com.google.common.collect.ImmutableList;
import gov.sandia.cognition.collection.CollectionUtil;
import gov.sandia.cognition.learning.algorithm.tree.CategorizationTree;
import gov.sandia.cognition.learning.algorithm.tree.DecisionTreeNode;
import gov.sandia.cognition.learning.data.DefaultInputOutputPair;
import gov.sandia.cognition.learning.data.InputOutputPair;
import gov.sandia.cognition.learning.data.RandomDataPartitioner;
import gov.sandia.cognition.learning.experiment.RandomFoldCreator;
import gov.sandia.cognition.learning.performance.categorization.ConfusionMatrix;
import gov.sandia.cognition.learning.performance.categorization.ConfusionMatrixPerformanceEvaluator;
import gov.sandia.cognition.learning.performance.categorization.DefaultConfusionMatrix;
import gov.sandia.cognition.math.matrix.Vector;
import org.apache.mahout.math.Matrix;

import java.util.Collection;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

abstract class AbstractMCFS {

    private final int numTrees;
    private final Random random;
    private final double v;
    private final double trainingPercent;

    protected final double u;

    public AbstractMCFS(int numTrees, Random random, double u, double v) {
        checkArgument(numTrees > 0, "Expected positive numTrees, but got: %s", numTrees);
        checkArgument(u > 0); // TODO: correct?
        checkArgument(v > 0); // TODO: correct?

        this.numTrees = numTrees;
        this.random = checkNotNull(random, "Expected random not to be null");
        this.u = u;
        this.v = v;
        this.trainingPercent = 0.66;
    }

    public AbstractMCFS(int numTrees, Random random, double u, double v, double trainingPercent) {
        checkArgument(numTrees > 0, "Expected positive numTrees, but got: %s", numTrees);
        checkArgument(u > 0); // TODO: correct?
        checkArgument(v > 0); // TODO: correct?
        checkArgument(0 < trainingPercent && trainingPercent < 1);

        this.numTrees = numTrees;
        this.random = checkNotNull(random, "Expected random not to be null");
        this.u = u;
        this.v = v;
        this.trainingPercent = trainingPercent;
    }

    public abstract double[] getScores(final Matrix table);

    protected SupervisedLearnerValidationExperimentStoringModels<Vector, Integer, ConfusionMatrix<Integer>, DefaultConfusionMatrix<Integer>>
            createAndRunExperiment(Matrix table) {
        List<Vector> objects = TreeExperiments.extractObjects(table);
        List<Integer> targets = TreeExperiments.extractTargets(table);

        List<DefaultInputOutputPair<Vector, Integer>> labeledDataset =
                DefaultInputOutputPair.mergeCollections(objects, targets);

        ConfusionMatrixPerformanceEvaluator<Vector, Integer> confMatEval = new ConfusionMatrixPerformanceEvaluator<>();
        DefaultConfusionMatrix.CombineSummarizer<Integer> confMatSumm =
                new DefaultConfusionMatrix.CombineSummarizer<>();

        RandomFoldCreator<InputOutputPair<Vector, Integer>> foldCreator =
                new RandomFoldCreator<>(
                        numTrees,
                        new RandomDataPartitioner<InputOutputPair<Vector, Integer>>(trainingPercent, random)
                );
        SupervisedLearnerValidationExperimentStoringModels<Vector, Integer, ConfusionMatrix<Integer>, DefaultConfusionMatrix<Integer>>
                experiment =
                new SupervisedLearnerValidationExperimentStoringModels<>(foldCreator, confMatEval, confMatSumm);

        VectorThresholdInformationGainLearnerStoringGain<Integer> deciderLearner =
                new VectorThresholdInformationGainLearnerStoringGain<>();
        CategorizationTreeLearnerStoringCardinality<Vector, Integer> treeLearner =
                new CategorizationTreeLearnerStoringCardinality<>(deciderLearner);

        experiment.evaluatePerformance(treeLearner, labeledDataset);

        return experiment;
    }

    protected double calculateWAcc(ConfusionMatrix<Integer> confMat) {

        double zeros = confMat.getCount(0, 0) / (confMat.getCount(0, 0) + confMat.getCount(0, 1));
        double ones = confMat.getCount(1, 1) / (confMat.getCount(1, 0) + confMat.getCount(1, 1));

        return 0.5 * (zeros + ones);
    }

    protected void traverseTree(CategorizationTree<Vector, Integer> tree, double[] scores) {
        CategorizationTreeNodeWithCardinality<Vector, Integer, Double> rootNode =
                (CategorizationTreeNodeWithCardinality<Vector, Integer, Double>) tree.getRootNode();

        traverse(ImmutableList.of((DecisionTreeNode) rootNode), scores, rootNode.getCardinality());
    }

    protected void traverse(
            Collection<DecisionTreeNode> nodes,
            double[] scores,
            int rootCard) {

        if (CollectionUtil.isEmpty(nodes)) {
            return;
        }

        for (DecisionTreeNode node : nodes) {

            CategorizationTreeNodeWithCardinality nodeWithCard =
                    (CategorizationTreeNodeWithCardinality) node;

            int card = nodeWithCard.getCardinality();

            VectorElementThresholdCategorizerWithGain decider =
                    (VectorElementThresholdCategorizerWithGain) nodeWithCard.getDecider();

            if (decider == null) { // leaf node
                continue;
            }

            double gain = decider.getGain();
            int attr = decider.getIndex();

            scores[attr] += gain * Math.pow((double) card / rootCard, v);

            @SuppressWarnings("unchecked")
            Collection<DecisionTreeNode> children = node.getChildren();
            traverse(children, scores, rootCard);
        }
    }
}
