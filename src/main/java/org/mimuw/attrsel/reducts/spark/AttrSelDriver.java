package org.mimuw.attrsel.reducts.spark;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.mimuw.attrsel.common.CSVMatrixReader;
import org.mimuw.attrsel.common.SubtableWritable;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.mimuw.attrsel.reducts.AbstractAttrSelReductsDriver;
import org.mimuw.attrsel.reducts.FrequencyScoreCalculator;
import org.mimuw.attrsel.reducts.RandomReducts;
import org.mimuw.attrsel.reducts.RsesDiscretizer;
import rseslib.processing.discretization.ChiMergeDiscretizationProvider;
import rseslib.processing.reducts.ReductsProvider;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class AttrSelDriver extends AbstractAttrSelReductsDriver implements Serializable {

    @Override
    public int run(String[] args) throws Exception {
        setUpAttrSelOptions();
        setUpReductsOptions();

        if (parseArguments(args, false, true) == null) {
            return 1;
        }

        // have to get options here not to serialize this
        final boolean dontDiscretize = hasOption("dontDiscretize");
        final Class<? extends ReductsProvider> reductsProviderClass = getReductsProviderClass();
        final RandomReducts.IndiscernibilityForMissing indiscernibilityForMissing = getIndiscernibilityForMissing();
        final RandomReducts.DiscernibilityMethod discernibilityMethod = getDiscernibilityMethod();
        final RandomReducts.GeneralizedDecisionTransitiveClosure generalizedDecisionTransitiveClosure =
                getGeneralizedDecisionTransitiveClosure();
        final RandomReducts.JohnsonReducts johnsonReducts = getJohnsonReducts();
        final int numDiscIntervals = getInt("numDiscIntervals", 4);
        final Double discSignificance = Double.valueOf(getOption("discSignificance", "0.25"));


        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);

        SerializableMatrix inputDataTable = readInputMatrix(sc);

        SubtableGenerator<Subtable> subtableGenerator = getSubtableGenerator(inputDataTable.get());
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
                RandomReducts randomReducts = dontDiscretize ?
                        // cannot use getRandomReducts, because there are problems serializing AttrSelDriver
                        new RandomReducts(
                                subtable.get(),
                                reductsProviderClass,
                                indiscernibilityForMissing,
                                discernibilityMethod,
                                generalizedDecisionTransitiveClosure,
                                johnsonReducts
                        ) :
                        new RandomReducts(
                                subtable.get(),
                                reductsProviderClass,
                                new RsesDiscretizer(
                                        new ChiMergeDiscretizationProvider(numDiscIntervals, discSignificance)
                                ),
                                indiscernibilityForMissing,
                                discernibilityMethod,
                                generalizedDecisionTransitiveClosure,
                                johnsonReducts
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

        double[] scoresArr = new double[inputDataTable.get().columnSize() - 1];

        for (Tuple2<Integer, Double> tup : result) {
            scoresArr[tup._1()] = tup._2();
        }

        printScoresAssessResults(scoresArr, inputDataTable.get());

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

    public static void main(String... args) throws Exception {
        new AttrSelDriver().run(args);
    }
}