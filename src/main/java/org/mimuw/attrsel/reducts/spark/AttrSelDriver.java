package org.mimuw.attrsel.reducts.spark;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.Matrix;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.mimuw.attrsel.common.CSVMatrixReader;
import org.mimuw.attrsel.common.MatrixFixedSizeObjectSubtableGenerator;
import org.mimuw.attrsel.common.MemoryGauge;
import org.mimuw.attrsel.common.SubtableWritable;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.mimuw.attrsel.reducts.FrequencyScoreCalculator;
import org.mimuw.attrsel.reducts.RandomReducts;
import org.mimuw.attrsel.reducts.RsesDiscretizer;
import org.slf4j.LoggerFactory;
import rseslib.processing.discretization.ChiMergeDiscretizationProvider;
import rseslib.processing.reducts.JohnsonReductsProvider;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import static com.google.common.base.Preconditions.checkArgument;

public final class AttrSelDriver extends AbstractJob implements Serializable {

    public static final MetricRegistry METRICS = new MetricRegistry();
    static {
        METRICS.register("MemoryGauge", new MemoryGauge());
        Slf4jReporter.forRegistry(METRICS)
                .outputTo(LoggerFactory.getLogger(name(AttrSelDriver.class, "Metrics")))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build()
                .start(5, TimeUnit.SECONDS);
    }

    @Override
    public int run(String[] args) throws Exception {
        // general options
        addInputOption(); // TODO: hack - input is treated as local-fs input, not hdfs
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


        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);

        SerializableMatrix inputDataTable = readInputMatrix(sc);
        SubtableGenerator<Subtable> subtableGenerator = getSubtableGenerator(inputDataTable);

        List<SubtableWritable> subtablesWritable = getWritableSubtables(subtableGenerator);

        // broadcast number of sub per attr
        List<Integer> tmpNumSubPerAttr = subtableGenerator.getNumberOfSubtablesPerAttribute();
        tmpNumSubPerAttr = new ArrayList<>(tmpNumSubPerAttr); // tmpNumSubPerAttr is not Serializable
        final Broadcast<List<Integer>> numSubPerAttr = sc.broadcast(tmpNumSubPerAttr);

        JavaRDD<SubtableWritable> distSubtables = sc.parallelize(subtablesWritable);

        // this gives reducts for each subtable
        JavaRDD<List<Integer>> reducts = distSubtables.flatMap(new FlatMapFunction<SubtableWritable, List<Integer>>() {
            @Override
            public Iterable<List<Integer>> call(SubtableWritable subtable) throws Exception {
                RandomReducts randomReducts =
                        // TODO: make all params configurable
                        // there are some problems with serialization of the whole AttrSelDriver
                        new RandomReducts(
                                subtable.get(),
                                JohnsonReductsProvider.class,
                                new RsesDiscretizer(new ChiMergeDiscretizationProvider(4, 0.2)),
                                RandomReducts.IndiscernibilityForMissing.DiscernFromValue,
                                RandomReducts.DiscernibilityMethod.OrdinaryDecisionAndInconsistenciesOmitted,
                                RandomReducts.GeneralizedDecisionTransitiveClosure.TRUE,
                                RandomReducts.JohnsonReducts.All
                        );
                return randomReducts.getReducts();
            }
        });

        // this gives pairs (attr, reduct) for all attr in each reduct
        JavaPairRDD<Integer, List<Integer>> attrReduct = reducts.flatMapToPair(
                new PairFlatMapFunction<List<Integer>, Integer, List<Integer>>() {
                    @Override
                    public Iterable<Tuple2<Integer, List<Integer>>> call(List<Integer> reduct) throws Exception {
                        List<Tuple2<Integer, List<Integer>>> result = new ArrayList<>(reduct.size());
                        for (int attr : reduct) {
                            result.add(new Tuple2<>(attr, reduct));
                        }
                        return result;
                    }
                }
        );

        JavaPairRDD<Integer, Iterable<List<Integer>>> attrReducts = attrReduct.groupByKey();

        // gives scores for each attribute
        JavaPairRDD<Integer, Double> scores = attrReducts.mapToPair(
                new PairFunction<Tuple2<Integer, Iterable<List<Integer>>>, Integer, Double>() {
                    @Override
                    public Tuple2<Integer, Double> call(Tuple2<Integer, Iterable<List<Integer>>> val) throws Exception {
                        return new Tuple2<>(
                                val._1(),
                                new FrequencyScoreCalculator(val._2(), numSubPerAttr.value().get(val._1())).getScore()
                        );
                    }
                }
        );

        List<Tuple2<Integer, Double>> result = scores.collect();

        System.out.println(result);

        // TODO: add the actual selection of the attributes and formated printing

        return 0;
    }

    private SerializableMatrix readInputMatrix(JavaSparkContext sc) {
        JavaPairRDD<String, String> inputData = sc.wholeTextFiles(getInputPath().toString());
        // read each whole file into a matrix
        JavaRDD<SerializableMatrix> inputMatrix = inputData.flatMap(
                new FlatMapFunction<Tuple2<String, String>, SerializableMatrix>() {
                    @Override
                    public Iterable<SerializableMatrix> call(Tuple2<String, String> in) throws Exception {
                        return ImmutableList.of(
                                SerializableMatrix.of(
                                        new CSVMatrixReader().read(new ByteArrayInputStream(in._2().getBytes()))
                                )
                        );
                    }
                }
        );

        // make a local Matrix out of the only file
        List<SerializableMatrix> matrices = inputMatrix.collect();
        checkArgument(matrices.size() == 1, "Expected one input file, but got: %s", matrices.size());
        return matrices.get(0);
    }

    private SubtableGenerator<Subtable> getSubtableGenerator(SerializableMatrix inputDataTable) throws Exception {
        @SuppressWarnings("unchecked")
        Class<SubtableGenerator<Subtable>> generatorClass =
                (Class<SubtableGenerator<Subtable>>) Class.forName(
                       getOption("subtableGenerator", MatrixFixedSizeObjectSubtableGenerator.class.getCanonicalName()));

        int numberOfSubtables = getInt("numSubtables");
        int subtableSize = getInt("subtableCardinality");
        long seed = Long.parseLong(getOption("seed"));

        return generatorClass
                .getConstructor(Random.class, int.class, int.class, Matrix.class)
                .newInstance(RandomUtils.getRandom(seed), numberOfSubtables, subtableSize, inputDataTable.get());
    }

    private List<SubtableWritable> getWritableSubtables(SubtableGenerator<Subtable> subtableGenerator) {
        List<Subtable> subtables = subtableGenerator.getSubtables();
        return Lists.transform(subtables,
                new Function<Subtable, SubtableWritable>() {
                    @Nullable
                    @Override
                    public SubtableWritable apply(@Nullable Subtable input) {
                        return new SubtableWritable(input);
                    }
                }
        );
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
        new AttrSelDriver().run(args);
    }
}