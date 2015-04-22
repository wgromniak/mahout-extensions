package org.mimuw.attrsel.trees.standalone;

import org.apache.commons.lang.ArrayUtils;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.Matrix;
import org.mimuw.attrsel.common.CSVMatrixReader;
import org.mimuw.attrsel.common.FastCutoffPoint;
import org.mimuw.attrsel.common.TreeAccuracyValidator;
import org.mimuw.attrsel.common.api.CutoffPointCalculator;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.mimuw.attrsel.trees.MCFS;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkState;

final class TreeStandaloneDriver extends AbstractJob {

    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    @Override
    public int run(String... args) throws Exception {
        // general options
        addInputOption(); // TODO: hack - input is treated as local-fs input, not hdfs
        addOutputOption();
        addOption("numSubtables", "numSub", "Number of subtables the original tables will be divided into", true);
        addOption("subtableCardinality", "subCard", "Cardinality of each of the subtables", true);
        addOption("subtableGenerator", "subGen", "Class of the subtable generator");
        addOption("seed", "seed", "Random number generator seed", "123456789");

        // mcfs options
        addOption("numberOfTrees", "numTrees", "Number of trees in each map task");
        addOption("u", "u", "u param from the paper");
        addOption("v", "v", "v param from the paper");

        Map<String, List<String>> parsedArgs = parseArguments(args, true, true);

        if (parsedArgs == null) {
            return 1;
        }

        Matrix inputDataTable = new CSVMatrixReader().read(Paths.get(getInputFile().getPath()));

        @SuppressWarnings("unchecked")
        Class<SubtableGenerator<Subtable>> generatorClass =
                (Class<SubtableGenerator<Subtable>>)
                        Class.forName(
                                getOption(
                                        "subtableGenerator",
                                        "org.mimuw.attrsel.common.MatrixFixedSizeAttributeSubtableGenerator"
                                )
                        );

        int numberOfSubtables = getInt("numSubtables");
        int subtableSize = getInt("subtableCardinality");
        long seed = Long.parseLong(getOption("seed"));

        SubtableGenerator<Subtable> subtableGenerator = generatorClass
                .getConstructor(Random.class, int.class, int.class, Matrix.class)
                .newInstance(RandomUtils.getRandom(seed), numberOfSubtables, subtableSize, inputDataTable);

        List<Subtable> subtables = subtableGenerator.getSubtables();

        final List<Callable<double[]>> map = new ArrayList<>(subtables.size());

        // TODO: use ParallelMCFS?
        final MCFS mcfs = new MCFS(
                getInt("numTrees", 100),
                RandomUtils.getRandom(Long.valueOf(getOption("seed"))),
                Double.valueOf(getOption("u", "2")),
                Double.valueOf(getOption("u", "2"))
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

        double[] scores = new double[inputDataTable.columnSize() - 1];

        for (int i = 0, n = mapResult.size(); i < n; i++) {
            double[] smallScores = mapResult.get(i).get();
            Subtable subtable = subtables.get(i);

            for (int j = 0; j < smallScores.length; j++) {
                scores[subtable.getAttributeAtPosition(j)] = smallScores[j];
            }
        }

        System.out.println("Scores are (<attr>: <score>):");
        for (int i = 0; i < scores.length; i++) {
            System.out.printf("%s: %s%n", i, scores[i]);
        }

        CutoffPointCalculator cutoffCalculator = new FastCutoffPoint();
        List<Integer> selected = cutoffCalculator.calculateCutoffPoint(Arrays.asList(ArrayUtils.toObject(scores)));

        System.out.printf("Selected attrs: %s%n", selected);

        double acc = new TreeAccuracyValidator().validate(inputDataTable, selected);

        System.out.printf("Accuracy: %s%n", acc);

        executor.shutdown();
        return 0;
    }

    public static void main(String... args) throws Exception {
        new TreeStandaloneDriver().run(args);
    }
}
