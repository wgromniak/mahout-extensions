package org.mimuw.attrsel.reducts.spark;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.mimuw.attrsel.common.CSVMatrixReader;
import org.mimuw.attrsel.common.MatrixFixedSizeAttributeSubtableGenerator;
import org.mimuw.attrsel.common.MemoryGauge;
import org.mimuw.attrsel.common.SubtableWritable;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.mimuw.attrsel.reducts.FrequencyScoreCalculator;
import org.mimuw.attrsel.reducts.RsesSubtableConverter;
import org.slf4j.LoggerFactory;
import rseslib.processing.reducts.JohnsonReductsProvider;
import rseslib.processing.reducts.ReductsProvider;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * TODO: Dirty and ugly.
 */
public final class AttrSelDriver {

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

    public static void main(String... args) throws Exception {

        final Properties properties = new Properties();

        // TODO: make configurable via commandline args
        properties.setProperty("IndiscernibilityForMissing", "DiscernFromValue");
        properties.setProperty("DiscernibilityMethod", "OrdinaryDecisionAndInconsistenciesOmitted");
        properties.setProperty("GeneralizedDecisionTransitiveClosure", "TRUE");
        properties.setProperty("JohnsonReducts", "All");

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);

        checkArgument(args.length == 3, "Expected 3 commandline arguments, but got: %s", args.length);

        JavaPairRDD<String, String> inputData = sc.wholeTextFiles(args[0]);
        JavaRDD<SerializableMatrix> inputMatrix = inputData.flatMap(new FlatMapFunction<Tuple2<String, String>, SerializableMatrix>() {
            @Override
            public Iterable<SerializableMatrix> call(Tuple2<String, String> in) throws Exception {
                return ImmutableList.of(SerializableMatrix.of(new CSVMatrixReader().read(new ByteArrayInputStream(in._2().getBytes()))));
            }
        });

        SerializableMatrix inputDataTable = inputMatrix.collect().get(0); // should be exactly one

        SubtableGenerator<Subtable> subtableGenerator
                = new MatrixFixedSizeAttributeSubtableGenerator(new Random(1234), Integer.parseInt(args[1]), Integer.parseInt(args[2]), inputDataTable.get());

        List<Subtable> subtables = subtableGenerator.getSubtables();
        List<SubtableWritable> subtablesWritable = Lists.transform(subtables, new Function<Subtable, SubtableWritable>() {
            @Nullable
            @Override
            public SubtableWritable apply(@Nullable Subtable input) {
                return new SubtableWritable(input);
            }
        });

        List<Integer> tmpNumSubPerAttr = subtableGenerator.getNumberOfSubtablesPerAttribute();

        tmpNumSubPerAttr = new ArrayList<>(tmpNumSubPerAttr);

        final Broadcast<List<Integer>> numSubPerAttr = sc.broadcast(tmpNumSubPerAttr);

        JavaRDD<SubtableWritable> distSubtables = sc.parallelize(subtablesWritable);

        JavaRDD<Reduct> reducts = distSubtables.flatMap(new FlatMapFunction<SubtableWritable, Reduct>() {
            @Override
            public Iterable<Reduct> call(SubtableWritable subtable) throws Exception {

                ReductsProvider reductsProvider = new JohnsonReductsProvider(properties, RsesSubtableConverter.getInstance().convert(subtable.get()));

                Collection<BitSet> reducts = reductsProvider.getReducts();

                List<Reduct> listReducts = new ArrayList<>(reducts.size());

                for (BitSet actualReduct : reducts) {

                    List<Integer> listReduct = new ArrayList<>();

                    for (int i = actualReduct.nextSetBit(0); i >= 0; i = actualReduct.nextSetBit(i + 1)) {

                        listReduct.add(subtable.get().getAttributeAtPosition(i));
                    }

                    for (int attr : listReduct) {
                        listReducts.add(new Reduct(attr, listReduct));
                    }
                }

                return listReducts;
            }
        });

        JavaPairRDD<Integer, List<Integer>> attrReduct = reducts.mapToPair(new PairFunction<Reduct, Integer, List<Integer>>() {
            @Override
            public Tuple2<Integer, List<Integer>> call(Reduct reduct) throws Exception {
                return new Tuple2<>(reduct.attr, reduct.reduct);
            }
        });

        JavaPairRDD<Integer, Iterable<List<Integer>>> attrReducts = attrReduct.groupByKey();

        JavaPairRDD<Integer, Double> scores = attrReducts.mapToPair(new PairFunction<Tuple2<Integer, Iterable<List<Integer>>>, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Iterable<List<Integer>>> val) throws Exception {
                return new Tuple2<>(val._1(), new FrequencyScoreCalculator(val._2(), numSubPerAttr.value().get(val._1())).getScore());
            }
        });

        List<Tuple2<Integer, Double>> collected = scores.collect();

        System.out.println(collected);
    }

    private static final class Reduct {

        private final int attr;
        private final List<Integer> reduct;

        private Reduct(int attr, List<Integer> reduct) {
            this.attr = attr;
            this.reduct = reduct;
        }
    }
}