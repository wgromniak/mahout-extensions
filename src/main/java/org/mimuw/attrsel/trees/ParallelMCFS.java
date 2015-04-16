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
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ParallelMCFS extends AbstractMCFS {

    private final ExecutorService executor;

    public ParallelMCFS(int numTrees, Random random, double u, double v, ExecutorService executor) {
        super(numTrees, random, u, v);

        this.executor = checkNotNull(executor, "Expected executor not to be null");
    }

    public ParallelMCFS(
            int numTrees,
            Random random,
            double u,
            double v,
            double trainingPercent,
            ExecutorService executor) {
        super(numTrees, random, u, v, trainingPercent);

        this.executor = checkNotNull(executor, "Expected executor not to be null");
    }

    public double[] getScores(final Matrix table) {

        CompletionService<double[]> completionService = new ExecutorCompletionService<>(executor);

        SupervisedLearnerValidationExperimentStoringModels<Vector, Integer, ConfusionMatrix<Integer>, DefaultConfusionMatrix<Integer>>
                experiment = createAndRunExperiment(table);

        final List<Evaluator<? super Vector, Integer>> trees = experiment.getLearned();
        final ArrayList<ConfusionMatrix<Integer>> statistics = experiment.getStatistics();

        checkState(trees.size() == statistics.size(),
                "Num trees=%s != %s=num statistics", trees.size(), statistics.size());

        double[] totalScores = new double[table.columnSize() - 1];

        for (int i = 0, n = trees.size(); i < n; i++) {
            final int tmp = i;

            completionService.submit(new Callable<double[]>() {
                @Override
                public double[] call() throws Exception {

                    double wAcc = calculateWAcc(statistics.get(tmp));

                    if (Double.isNaN(wAcc)) {
                        // results will be excluded for this fold
                        return null;
                    }

                    double[] scores = new double[table.columnSize() - 1];

                    @SuppressWarnings("unchecked")
                    CategorizationTree<Vector, Integer> tree = (CategorizationTree<Vector, Integer>) trees.get(tmp);
                    traverseTree(tree, scores);

                    for (int j = 0; j < scores.length; j++) {
                        scores[j] = Math.pow(wAcc, u) * scores[j];
                    }

                    return scores;
                }
            });
        }

        try {
            for (int i = 0, n = trees.size(); i < n; i++) {

                Future<double[]> result = completionService.take();
                double[] scores = result.get();

                if (scores == null) {
                    continue;
                }

                for (int j = 0; j < scores.length; j++) {
                    totalScores[j] += scores[j];
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw launderThrowable(e.getCause());
        }

        return totalScores;
    }

    /**
     * If the Throwable is an Error, throw it; if it is a RuntimeException return it, otherwise throw
     * IllegalStateException.
     *
     * <p> Borrowed from "Java concurrency in practice".
     */
    public static RuntimeException launderThrowable(Throwable t) {
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        } else {
            throw new IllegalStateException("Not unchecked", t);
        }
    }

    public static void main(String... args) {

        Matrix mat = new CSVMatrixReader().read(Paths.get("res", "in", "wekaGen.csv"));

        ExecutorService exec = Executors.newFixedThreadPool(4);

        ParallelMCFS mcfs = new ParallelMCFS(10, new Random(1234), 2, 2, exec);

        double[] scores = mcfs.getScores(mat);

        System.out.println("scores = " + Arrays.toString(scores));

        exec.shutdown();
    }
}
