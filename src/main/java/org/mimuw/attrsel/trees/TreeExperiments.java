package org.mimuw.attrsel.trees;

import gov.sandia.cognition.learning.algorithm.tree.CategorizationTree;
import gov.sandia.cognition.learning.data.DefaultInputOutputPair;
import gov.sandia.cognition.learning.data.InputOutputPair;
import gov.sandia.cognition.learning.experiment.CrossFoldCreator;
import gov.sandia.cognition.learning.performance.categorization.ConfusionMatrix;
import gov.sandia.cognition.learning.performance.categorization.ConfusionMatrixPerformanceEvaluator;
import gov.sandia.cognition.learning.performance.categorization.DefaultConfusionMatrix;
import gov.sandia.cognition.math.matrix.Vector;
import gov.sandia.cognition.math.matrix.mtj.DenseVectorFactoryMTJ;
import org.apache.mahout.math.Matrix;
import org.mimuw.attrsel.common.CSVMatrixReader;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class TreeExperiments {

    public static void main(String... args) {

        ConfusionMatrixPerformanceEvaluator<Vector, Integer> confMatEval = new ConfusionMatrixPerformanceEvaluator<>();
        DefaultConfusionMatrix.CombineSummarizer<Integer> confMatSumm =
                new DefaultConfusionMatrix.CombineSummarizer<>();

        Matrix mat = new CSVMatrixReader().read(Paths.get("res", "in", "wekaGen.csv")); // enter your data here

        List<Vector> objects = extractObjects(mat);
        List<Integer> targets = extractTargets(mat);

        List<DefaultInputOutputPair<Vector, Integer>> labeledDataset =
                DefaultInputOutputPair.mergeCollections(objects, targets);

        CrossFoldCreator<InputOutputPair<Vector, Integer>> foldCreator = new CrossFoldCreator<>(10, new Random(123));
        SupervisedLearnerValidationExperimentStoringModels
                <Vector, Integer, ConfusionMatrix<Integer>, DefaultConfusionMatrix<Integer>>
                experiment =
                new SupervisedLearnerValidationExperimentStoringModels<>(foldCreator, confMatEval, confMatSumm);

        VectorThresholdInformationGainLearnerStoringGain<Integer> deciderLearner =
                new VectorThresholdInformationGainLearnerStoringGain<>();
        CategorizationTreeLearnerStoringCardinality<Vector, Integer> treeLearner =
                new CategorizationTreeLearnerStoringCardinality<>(deciderLearner);

        // this will run a cross validation experiment with the tree
        DefaultConfusionMatrix<Integer> confMat = experiment.evaluatePerformance(treeLearner, labeledDataset);
        System.out.println(confMat);


        // this will learn the tree on whole data for further classification
        CategorizationTree<Vector, Integer> tree = treeLearner.learn(labeledDataset);
        System.out.println(
                ((VectorElementThresholdCategorizerWithGain)
                        ((CategorizationTreeNodeWithCardinality) tree.getRootNode().getChildren().iterator().next()).getDecider()
                                ).getCategories());
        Integer clz = tree.evaluate(DenseVectorFactoryMTJ.INSTANCE.copyValues(1, 1, 0, 1, 1, 0, 0, 1, 1, 0));
        System.out.println(clz);
    }

    public static List<Vector> extractObjects(Matrix mat) {

        List<Vector> vecs = new ArrayList<>(mat.columnSize());

        for (org.apache.mahout.math.Vector vec : mat) {

            vecs.add(vecToVec(vec.viewPart(0, vec.size() - 1)));
        }

        return vecs;
    }

    public static Vector vecToVec(org.apache.mahout.math.Vector source) {

        Vector dest = DenseVectorFactoryMTJ.INSTANCE.createVector(source.size());

        for (int i = 0; i < source.size(); i++) {

            dest.setElement(i, source.getQuick(i));
        }

        return dest;
    }

    public static List<Integer> extractTargets(Matrix mat) {

        org.apache.mahout.math.Vector lastCol = mat.viewColumn(mat.columnSize() - 1);

        List<Integer> intCol = new ArrayList<>(lastCol.size());

        for (int i = 0; i < lastCol.size(); i++) {

            intCol.add((int) lastCol.getQuick(i));
        }

        return intCol;
    }
}
