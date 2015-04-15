package org.mimuw.attrsel.reducts.standalone;

import org.apache.commons.lang.ArrayUtils;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.Matrix;
import org.mimuw.attrsel.common.CSVMatrixReader;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.mimuw.attrsel.reducts.FastCutoffPoint;
import org.mimuw.attrsel.reducts.RandomReducts;
import org.mimuw.attrsel.reducts.api.CutoffPointCalculator;
import rseslib.processing.reducts.JohnsonReductsProvider;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class ReductsStandaloneDriver extends AbstractJob {

    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    @Override
    public int run(String... args) throws Exception {
        addInputOption(); // TODO: hack - input is treated as local-fs input, not hdfs
        addOption("numSubtables", "numSub", "Number of subtables the original tables will be divided into", true);
        addOption("subtableCardinality", "subCard", "Cardinality of each of the subtables", true);
        addOption(
                "subtableGenerator",
                "subGen",
                "Class of the subtable generator",
                "org.mimuw.attrsel.common.MatrixFixedSizeAttributeSubtableGenerator"
        );
        addOption("seed", "seed", "Random number generator seed", "123456789");
        Map<String, List<String>> parsedArgs = parseArguments(args, true, true);

        if (parsedArgs == null) {
            return 1;
        }

        Matrix inputDataTable = new CSVMatrixReader().read(Paths.get(getInputFile().getPath()));

        @SuppressWarnings("unchecked")
        Class<SubtableGenerator<Subtable>> generatorClass =
                (Class<SubtableGenerator<Subtable>>) Class.forName(getOption("subtableGenerator"));

        int numberOfSubtables = getInt("numSubtables");
        int subtableSize = getInt("subtableCardinality");
        long seed = Long.parseLong(getOption("seed"));

        SubtableGenerator<Subtable> subtableGenerator;

        subtableGenerator = generatorClass
                .getConstructor(Random.class, int.class, int.class, Matrix.class)
                .newInstance(RandomUtils.getRandom(seed), numberOfSubtables, subtableSize, inputDataTable);

        List<Subtable> subtables = subtableGenerator.getSubtables();
        List<Integer> numberOfSubtablesPerAttribute = subtableGenerator.getNumberOfSubtablesPerAttribute();

        List<Callable<List<List<Integer>>>> map = new ArrayList<>(subtables.size());

        for (final Subtable subtable : subtables) {
            map.add(new Callable<List<List<Integer>>>() {
                @Override
                public List<List<Integer>> call() throws Exception {
                    RandomReducts randomReducts =
                            new RandomReducts(
                                    subtable,
                                    JohnsonReductsProvider.class,
                                    RandomReducts.IndiscernibilityForMissing.DiscernFromValue,
                                    RandomReducts.DiscernibilityMethod.OrdinaryDecisionAndInconsistenciesOmitted,
                                    RandomReducts.GeneralizedDecisionTransitiveClosure.TRUE,
                                    RandomReducts.JohnsonReducts.All
                            );
                    return randomReducts.getReducts();
                }
            });
        }

        List<Future<List<List<Integer>>>> mapResults = executor.invokeAll(map);

        int[] attrCounts = new int[inputDataTable.columnSize() - 1];

        for (Future<List<List<Integer>>> result : mapResults) {
            for (List<Integer> reduct : result.get()) {
                for (int attr : reduct) {
                    attrCounts[attr] += 1;
                }
            }
        }

        double[] scores = new double[inputDataTable.columnSize() - 1];

        for (int i = 0; i < attrCounts.length; i++) {
            scores[i] = (double) attrCounts[i] / numberOfSubtablesPerAttribute.get(i);
        }

        System.out.println("Scores are (<attr>: <score>):");
        for (int i = 0; i < scores.length; i++) {
            System.out.printf("%s: %s%n", i, scores[i]);
        }

        CutoffPointCalculator cutoffCalculator = new FastCutoffPoint();
        List<Integer> selected = cutoffCalculator.calculateCutoffPoint(Arrays.asList(ArrayUtils.toObject(scores)));

        System.out.printf("Selected attrs: %s%n", selected);

        executor.shutdown();
        return 0;
    }

    public static void main(String... args) throws Exception {
        new ReductsStandaloneDriver()
                .run(
                        "-i", "res/in/wekaGen.csv",
                        "-numSub", "10000",
                        "-subCard", "66",
                        "-subGen", "org.mimuw.attrsel.common.MatrixFixedSizeObjectSubtableGenerator"
                );
    }
}
