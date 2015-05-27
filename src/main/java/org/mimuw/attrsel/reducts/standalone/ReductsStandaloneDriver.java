package org.mimuw.attrsel.reducts.standalone;

import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.mimuw.attrsel.reducts.AbstractAttrSelReductsDriver;
import org.mimuw.attrsel.reducts.RandomReducts;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

// TODO: configure logging in standalone mode
final class ReductsStandaloneDriver extends AbstractAttrSelReductsDriver {

    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    @Override
    public int run(String... args) throws Exception {
        setUpAttrSelOptions();
        setUpReductsOptions();

        if (parseArguments(args, false, true) == null) {
            return 1;
        }

        loadInputData();

        SubtableGenerator<Subtable> subtableGenerator = getSubtableGenerator();

        List<Subtable> subtables = subtableGenerator.getSubtables();
        List<Integer> numberOfSubtablesPerAttribute = subtableGenerator.getNumberOfSubtablesPerAttribute();

        List<Callable<List<List<Integer>>>> map = new ArrayList<>(subtables.size());

        for (final Subtable subtable : subtables) {
            map.add(new Callable<List<List<Integer>>>() {
                @Override
                public List<List<Integer>> call() throws Exception {
                    RandomReducts randomReducts = getRandomReducts(subtable);
                    return randomReducts.getReducts();
                }
            });
        }

        List<Future<List<List<Integer>>>> mapResults = executor.invokeAll(map);

        int[] attrCounts = new int[trainTable.columnSize() - 1];

        for (Future<List<List<Integer>>> result : mapResults) {
            for (List<Integer> reduct : result.get()) {
                for (int attr : reduct) {
                    attrCounts[attr] += 1;
                }
            }
        }

        double[] scores = new double[trainTable.columnSize() - 1];

        for (int i = 0; i < attrCounts.length; i++) {
            scores[i] = (double) attrCounts[i] / numberOfSubtablesPerAttribute.get(i);
        }

        printScoresAssessResults(scores);

        executor.shutdown();
        return 0;
    }


    public static void main(String... args) throws Exception {
        new ReductsStandaloneDriver().run(args);
    }
}
