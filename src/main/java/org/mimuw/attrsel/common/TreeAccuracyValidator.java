package org.mimuw.attrsel.common;

import gov.sandia.cognition.learning.algorithm.tree.CategorizationTreeLearner;
import gov.sandia.cognition.learning.algorithm.tree.VectorThresholdInformationGainLearner;
import gov.sandia.cognition.learning.data.DefaultInputOutputPair;
import gov.sandia.cognition.learning.data.InputOutputPair;
import gov.sandia.cognition.learning.experiment.CrossFoldCreator;
import gov.sandia.cognition.learning.experiment.SupervisedLearnerValidationExperiment;
import gov.sandia.cognition.learning.performance.categorization.ConfusionMatrix;
import gov.sandia.cognition.learning.performance.categorization.ConfusionMatrixPerformanceEvaluator;
import gov.sandia.cognition.learning.performance.categorization.DefaultConfusionMatrix;
import gov.sandia.cognition.math.matrix.Vector;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.attrsel.common.api.AttrSelValidator;
import org.mimuw.attrsel.trees.TreeExperiments;

import java.util.List;
import java.util.Random;

/**
 * Validates attribute selection learning trees on selected attributes and performing cross-validation. Returns accuracy
 * as a result.
 */
public final class TreeAccuracyValidator implements AttrSelValidator<Double> {

    private final int numFolds;
    private final Random random;

    public TreeAccuracyValidator() {
        this.numFolds = 10;
        this.random = RandomUtils.getRandom(0xDEADBEEF);
    }

    public TreeAccuracyValidator(int numFolds, Random random) {
        this.numFolds = numFolds;
        this.random = random;
    }

    @Override
    public Double validate(Matrix dataTable, List<Integer> selectedAttributes) {

        Matrix selected = getSelectedAttributesOnly(dataTable, selectedAttributes);

        ConfusionMatrixPerformanceEvaluator<Vector, Integer> confMatEval = new ConfusionMatrixPerformanceEvaluator<>();
        DefaultConfusionMatrix.CombineSummarizer<Integer> confMatSumm =
                new DefaultConfusionMatrix.CombineSummarizer<>();

        List<Vector> objects = TreeExperiments.extractObjects(selected);
        List<Integer> targets = TreeExperiments.extractTargets(selected);

        List<DefaultInputOutputPair<Vector, Integer>> labeledDataset =
                DefaultInputOutputPair.mergeCollections(objects, targets);

        CrossFoldCreator<InputOutputPair<Vector, Integer>> foldCreator = new CrossFoldCreator<>(numFolds, random);
        SupervisedLearnerValidationExperiment<Vector, Integer, ConfusionMatrix<Integer>, DefaultConfusionMatrix<Integer>>
                experiment =
                new SupervisedLearnerValidationExperiment<>(foldCreator, confMatEval, confMatSumm);

        VectorThresholdInformationGainLearner<Integer> deciderLearner =
                new VectorThresholdInformationGainLearner<>();
        CategorizationTreeLearner<Vector, Integer> treeLearner =
                new CategorizationTreeLearner<>(deciderLearner);

        // this will run a cross validation experiment with the tree
        DefaultConfusionMatrix<Integer> confMat = experiment.evaluatePerformance(treeLearner, labeledDataset);

        return confMat.getAccuracy();
    }

    private Matrix getSelectedAttributesOnly(Matrix dataTable, List<Integer> selectedAttributes) {

        Matrix selected = new DenseMatrix(dataTable.rowSize(), selectedAttributes.size() + 1);

        // assign selected attributes
        for (int i = 0, n = selectedAttributes.size(); i < n; i++) {
            selected.assignColumn(i, dataTable.viewColumn(selectedAttributes.get(i)));
        }

        // assign decisions
        selected.assignColumn(selectedAttributes.size(), dataTable.viewColumn(dataTable.columnSize() - 1));

        return selected;
    }
}
