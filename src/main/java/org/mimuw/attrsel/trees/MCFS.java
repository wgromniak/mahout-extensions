package org.mimuw.attrsel.trees;

import com.google.common.collect.ImmutableList;
import gov.sandia.cognition.collection.CollectionUtil;
import gov.sandia.cognition.evaluator.Evaluator;
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
import org.mimuw.attrsel.common.CSVMatrixReader;

import java.nio.file.Paths;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;

public class MCFS {

    private static final double TRAINING_PERCENT = 0.66; // as specified in the paper

    private final int numTrees;
    private final Random random;
    private final double u;
    private final double v;

    public MCFS(int numTrees, Random random, double u, double v) {
        this.numTrees = numTrees;
        this.random = random;
        this.u = u;
        this.v = v;
    }

    public double[] getScores(Matrix table) {

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
                        new RandomDataPartitioner<InputOutputPair<Vector, Integer>>(TRAINING_PERCENT, random)
                );
        SupervisedLearnerValidationExperimentStoringModels
                <Vector, Integer, ConfusionMatrix<Integer>, DefaultConfusionMatrix<Integer>>
                experiment =
                new SupervisedLearnerValidationExperimentStoringModels<>(foldCreator, confMatEval, confMatSumm);

        VectorThresholdInformationGainLearnerStoringGain<Integer> deciderLearner =
                new VectorThresholdInformationGainLearnerStoringGain<>();
        CategorizationTreeLearnerStoringCardinality<Vector, Integer> treeLearner =
                new CategorizationTreeLearnerStoringCardinality<>(deciderLearner);

        experiment.evaluatePerformance(treeLearner, labeledDataset);

        List<Evaluator<? super Vector, Integer>> trees = experiment.getLearned();
        ArrayList<ConfusionMatrix<Integer>> statistics = experiment.getStatistics();

        checkState(trees.size() == statistics.size(),
                "Num trees=%s != %s=num statistics", trees.size(), statistics.size());

        double[] totalScores = new double[table.columnSize() - 1];

        for (int i = 0; i < trees.size(); i++) {

            double[] scores = new double[table.columnSize() - 1];

            double wAcc = calculateWAcc(statistics.get(i));

            if (Double.isNaN(wAcc)) {
                // results excluded for this fold
                continue;
            }

            @SuppressWarnings("unchecked")
            CategorizationTree<Vector, Integer> tree = (CategorizationTree<Vector, Integer>) trees.get(i);
            traverseTree(tree, scores);

            for (int j = 0; j < scores.length; j++) {
                totalScores[j] += Math.pow(wAcc, u) * scores[j];
            }
        }

        return totalScores;
    }

    private double calculateWAcc(ConfusionMatrix<Integer> confMat) {

        double zeros = confMat.getCount(0, 0) / (confMat.getCount(0, 0) + confMat.getCount(0, 1));
        double ones = confMat.getCount(1, 1) / (confMat.getCount(1, 0) + confMat.getCount(1, 1));

        return 0.5 * (zeros + ones);
    }

    private void traverseTree(CategorizationTree<Vector, Integer> tree, double[] scores) {
        CategorizationTreeNodeWithCardinality<Vector, Integer, Double> rootNode =
                (CategorizationTreeNodeWithCardinality<Vector, Integer, Double>) tree.getRootNode();

        traverse(ImmutableList.of((DecisionTreeNode) rootNode), scores, rootNode.getCardinality());
    }

    private void traverse(
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

    public static void main(String... args) {

        Matrix mat = new CSVMatrixReader().read(Paths.get("res", "in", "wekaGen.csv"));

        MCFS mcfs = new MCFS(10, new Random(1234), 1, 1);

        double[] scores = mcfs.getScores(mat);

        System.out.println("scores = " + Arrays.toString(scores));
    }
}
