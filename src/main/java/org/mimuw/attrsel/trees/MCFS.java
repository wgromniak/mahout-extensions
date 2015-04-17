package org.mimuw.attrsel.trees;

import gov.sandia.cognition.evaluator.Evaluator;
import gov.sandia.cognition.learning.algorithm.tree.CategorizationTree;
import gov.sandia.cognition.learning.performance.categorization.ConfusionMatrix;
import gov.sandia.cognition.learning.performance.categorization.DefaultConfusionMatrix;
import gov.sandia.cognition.math.matrix.Vector;
import org.apache.mahout.math.Matrix;
import org.mimuw.attrsel.common.CSVMatrixReader;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkState;

/**
 * TODO: add comments, tests
 */
public class MCFS extends AbstractMCFS {

    public MCFS(int numTrees, Random random, double u, double v) {
        super(numTrees, random, u, v);
    }

    public MCFS(int numTrees, Random random, double u, double v, double trainingPercent) {
        super(numTrees, random, u, v, trainingPercent);
    }

    public double[] getScores(final Matrix table) {

        SupervisedLearnerValidationExperimentStoringModels<Vector, Integer, ConfusionMatrix<Integer>, DefaultConfusionMatrix<Integer>>
                experiment = createAndRunExperiment(table);

        final List<Evaluator<? super Vector, Integer>> trees = experiment.getLearned();
        final ArrayList<ConfusionMatrix<Integer>> statistics = experiment.getStatistics();

        checkState(trees.size() == statistics.size(),
                "Num trees=%s != %s=num statistics", trees.size(), statistics.size());

        double[] totalScores = new double[table.columnSize() - 1];

        for (int i = 0, n = trees.size(); i < n; i++) {

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

    public static void main(String... args) {

        Matrix mat = new CSVMatrixReader().read(Paths.get("res", "in", "wekaGen.csv"));

        MCFS mcfs = new MCFS(10, new Random(1234), 2, 2);

        double[] scores = mcfs.getScores(mat);

        System.out.println("scores = " + Arrays.toString(scores));
    }
}
