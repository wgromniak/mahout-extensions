package org.mimuw.attrsel.reducts.standalone;

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
import org.mimuw.attrsel.reducts.RandomReducts;
import org.mimuw.attrsel.reducts.RsesDiscretizer;
import rseslib.processing.discretization.ChiMergeDiscretizationProvider;
import rseslib.processing.reducts.JohnsonReductsProvider;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

final class ReductsStandaloneDriver extends AbstractJob {

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

        // reducts options
        addOption("IndiscernibilityForMissing", "indisc", "Indiscernibility for missing values");
        addOption("DiscernibilityMethod", "discMeth", "Discernibility method");
        addOption("GeneralizedDecisionTransitiveClosure", "genDec", "Generalized decision transitive closure");
        addOption("JohnsonReducts", "johnson", "Johnson reducts");

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
        List<Integer> numberOfSubtablesPerAttribute = subtableGenerator.getNumberOfSubtablesPerAttribute();

        List<Callable<List<List<Integer>>>> map = new ArrayList<>(subtables.size());

        for (final Subtable subtable : subtables) {
            map.add(new Callable<List<List<Integer>>>() {
                @Override
                public List<List<Integer>> call() throws Exception {
                    RandomReducts randomReducts =
                            new RandomReducts(
                                    subtable,
                                    JohnsonReductsProvider.class, // TODO: make configurable
                                    new RsesDiscretizer(new ChiMergeDiscretizationProvider(4, 0.2)), // TODO: make configurable
                                    getIndiscernibilityForMissing(),
                                    getDiscernibilityMethod(),
                                    getGeneralizedDecisionTransitiveClosure(),
                                    getJohnsonReducts()
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

        double acc = new TreeAccuracyValidator().validate(inputDataTable, selected);

        System.out.printf("Accuracy: %s%n", acc);

        executor.shutdown();
        return 0;
    }

    private RandomReducts.IndiscernibilityForMissing getIndiscernibilityForMissing() {
        return hasOption("IndiscernibilityForMissing") ?
                RandomReducts.IndiscernibilityForMissing.valueOf(getOption("IndiscernibilityForMissing")) :
                RandomReducts.IndiscernibilityForMissing.DiscernFromValue;
    }

    private RandomReducts.DiscernibilityMethod getDiscernibilityMethod() {
        return hasOption("DiscernibilityMethod") ?
                RandomReducts.DiscernibilityMethod.valueOf(getOption("DiscernibilityMethod")) :
                RandomReducts.DiscernibilityMethod.OrdinaryDecisionAndInconsistenciesOmitted;
    }

    private RandomReducts.GeneralizedDecisionTransitiveClosure getGeneralizedDecisionTransitiveClosure() {
        return hasOption("GeneralizedDecisionTransitiveClosure") ?
                RandomReducts.GeneralizedDecisionTransitiveClosure
                        .valueOf(getOption("GeneralizedDecisionTransitiveClosure")) :
                RandomReducts.GeneralizedDecisionTransitiveClosure.TRUE;
    }

    private RandomReducts.JohnsonReducts getJohnsonReducts() {
        return hasOption("JohnsonReducts") ?
                RandomReducts.JohnsonReducts.valueOf(getOption("JohnsonReducts")) :
                RandomReducts.JohnsonReducts.All;
    }

    public static void main(String... args) throws Exception {
        new ReductsStandaloneDriver()
                .run(
                        "-i", "res/in/wekaGen.csv",
                        "-numSub", "100",
                        "-subCard", "66",
                        "-subGen", "org.mimuw.attrsel.common.MatrixFixedSizeObjectSubtableGenerator"
                );
    }
}
