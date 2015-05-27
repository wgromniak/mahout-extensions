package org.mimuw.attrsel.trees.standalone;

import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.mimuw.attrsel.trees.AbstractAttrSelTreesDriver;
import org.mimuw.attrsel.trees.MCFS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkState;

final class TreeStandaloneDriver extends AbstractAttrSelTreesDriver {

    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    @Override
    public int run(String... args) throws Exception {
        setUpAttrSelOptions();
        setUpTreesOptions();

        if (parseArguments(args, false, true) == null) {
            return 1;
        }

        loadInputData();

        SubtableGenerator<Subtable> subtableGenerator = getSubtableGenerator();

        List<Subtable> subtables = subtableGenerator.getSubtables();

        final List<Callable<double[]>> map = new ArrayList<>(subtables.size());

        final MCFS mcfs = new MCFS(
                getInt("numTrees", 100),
                Long.valueOf(getOption("seed")),
                Double.valueOf(getOption("u", "2")),
                Double.valueOf(getOption("v", "2"))
        );

        for (final Subtable subtable : subtables) {
            map.add(new Callable<double[]>() {
                @Override
                public double[] call() throws Exception {
                    return mcfs.getScores(subtable.getTable());
                }
            });
        }

        List<Future<double[]>> mapResult = executor.invokeAll(map);

        checkState(mapResult.size() == subtables.size());

        double[] scores = new double[fullInputTable.columnSize() - 1];

        for (int i = 0, n = mapResult.size(); i < n; i++) {
            double[] smallScores = mapResult.get(i).get();
            Subtable subtable = subtables.get(i);

            for (int j = 0; j < smallScores.length; j++) {
                scores[subtable.getAttributeAtPosition(j)] += smallScores[j];
            }
        }

        printScoresAssessResults(scores);

        executor.shutdown();
        return 0;
    }

    public static void main(String... args) throws Exception {
        new TreeStandaloneDriver().run(args);
    }
}
