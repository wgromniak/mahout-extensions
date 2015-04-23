package org.mimuw.attrsel.common;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.Matrix;
import org.mimuw.attrsel.common.api.CutoffPointCalculator;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public abstract class AbstractAttrSelDriver extends AbstractJob {

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

    protected SubtableGenerator<Subtable> getSubtableGenerator(Matrix fullMatrix) {
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
                    .newInstance(RandomUtils.getRandom(seed), numberOfSubtables, subtableSize, fullMatrix);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException |
                IllegalAccessException e) {
            throw new IllegalStateException("Error instantiating subtable generator", e);
        }
    }

    protected void printScoresAssessResults(double[] scores, Matrix inputDataTable) {

        System.out.println("Scores are (<attr>: <score>):");
        for (int i = 0; i < scores.length; i++) {
            System.out.printf("%s: %s%n", i, scores[i]);
        }

        CutoffPointCalculator cutoffCalculator = new FastCutoffPoint();
        List<Integer> selected = cutoffCalculator.calculateCutoffPoint(Arrays.asList(ArrayUtils.toObject(scores)));

        System.out.printf("Selected attrs: %s%n", selected);

        double acc = new TreeAccuracyValidator().validate(inputDataTable, selected);

        System.out.printf("Accuracy: %s%n", acc);
    }
}
