package org.mimuw.attrsel.common;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import org.apache.commons.lang.ArrayUtils;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.attrsel.common.api.CutoffPointCalculator;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public abstract class AbstractAttrSelDriver extends AbstractJob {

    private final Random random = RandomUtils.getRandom(0xFEEL);

    protected Matrix fullInputTable;
    protected Matrix trainTable;
    private Matrix testTable;

    public static final MetricRegistry METRICS = new MetricRegistry();
    static { // set-up driver memory monitoring
        METRICS.register("MemoryGauge", new MemoryGauge());
        Slf4jReporter.forRegistry(METRICS)
                .outputTo(LoggerFactory.getLogger(name(AbstractAttrSelDriver.class, "Metrics")))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build()
                .start(5, TimeUnit.SECONDS);
    }

    protected void setUpAttrSelOptions() {
        addInputOption();
        addOutputOption();
        addOption("numSubtables", "numSub", "Number of subtables the original tables will be divided into", true);
        addOption("subtableCardinality", "subCard", "Cardinality of each of the subtables", true);
        addOption("subtableGenerator", "subGen", "Class of the subtable generator");
        addOption("seed", "seed", "Random number generator seed", "123456789");
        addOption("numCutoffIterations", "numCutIter", "Number of iterations of the cutoff procedure");
        addOption("split", "sp", "Train/test data split percent, e.g. 0.2 for 20% test data and 80% train");
    }

    protected void copyOptionsToConf() {
        for (String key : argMap.keySet()) {
            // TODO: hack
            String optionName = key.substring(2); // it comes with -- prepended
            String option = getOption(optionName);
            if (option == null) {
                getConf().setBoolean(optionName, true);
            } else {
                getConf().set(optionName, option);
            }
        }
    }

    protected void loadInputData() {

        if (fullInputTable == null) { // TODO: fix (hack required by spark)
            fullInputTable = new CSVMatrixReader().read(Paths.get(getInputFile().getPath()));
        }

        int testSize = (int) (fullInputTable.rowSize() * Double.valueOf(getOption("split", "0.2")));

        testTable = new DenseMatrix(testSize, fullInputTable.columnSize());
        trainTable = new DenseMatrix(fullInputTable.rowSize() - testSize, fullInputTable.columnSize());

        List<Integer> nbrs = ContiguousSet
                .create(Range.closedOpen(0, fullInputTable.rowSize()), DiscreteDomain.integers())
                .asList();
        nbrs = new ArrayList<>(nbrs);
        Collections.shuffle(nbrs, random);
        nbrs = nbrs.subList(0, testSize);

        for (int i = 0, test = 0, train = 0; i < fullInputTable.rowSize(); i++) {
            if (nbrs.contains(i)) {
                testTable.assignRow(test++, fullInputTable.viewRow(i));
            } else {
                trainTable.assignRow(train++, fullInputTable.viewRow(i));
            }
        }
    }

    protected SubtableGenerator<Subtable> getSubtableGenerator() {
        try {
            @SuppressWarnings("unchecked")
            Class<SubtableGenerator<Subtable>> generatorClass =
                    (Class<SubtableGenerator<Subtable>>) Class.forName(
                            getOption("subtableGenerator", MatrixFixedSizeObjectSubtableGenerator.class.getCanonicalName()));

            int numberOfSubtables = getInt("numSubtables");
            int subtableSize = getInt("subtableCardinality");
            long seed = Long.parseLong(getOption("seed"));

            return generatorClass
                    .getConstructor(Random.class, int.class, int.class, org.apache.mahout.math.Matrix.class)
                    .newInstance(RandomUtils.getRandom(seed), numberOfSubtables, subtableSize, trainTable);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException |
                IllegalAccessException e) {
            throw new IllegalStateException("Error instantiating subtable generator", e);
        }
    }

    protected void printScoresAssessResults(double[] scores) {

        System.out.println("Scores are (<attr>: <score>):");
        for (int i = 0; i < scores.length; i++) {
            System.out.printf("%s: %s%n", i, scores[i]);
        }

        CutoffPointCalculator cutoffCalculator = new FastCutoffPoint(getInt("numCutoffIterations", 1));
        List<Integer> selected = cutoffCalculator.calculateCutoffPoint(Arrays.asList(ArrayUtils.toObject(scores)));

        System.out.printf("Selected attrs: %s%n", selected);
        System.out.printf("Num selected attrs: %s%n", selected.size());

        double acc = new TreeAccuracyValidator().validate(testTable, selected);

        System.out.printf("Accuracy: %s%n", acc);
    }
}
