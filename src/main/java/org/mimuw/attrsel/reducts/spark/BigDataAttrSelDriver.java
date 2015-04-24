package org.mimuw.attrsel.reducts.spark;

import com.google.common.collect.Iterables;
import org.apache.commons.lang.ArrayUtils;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.mimuw.attrsel.common.FastCutoffPoint;
import org.mimuw.attrsel.common.ObjectSubtable;
import org.mimuw.attrsel.common.api.CutoffPointCalculator;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.reducts.AbstractAttrSelReductsDriver;
import org.mimuw.attrsel.reducts.FrequencyScoreCalculator;
import org.mimuw.attrsel.reducts.RandomReducts;
import org.mimuw.attrsel.reducts.RsesDiscretizer;
import rseslib.processing.discretization.ChiMergeDiscretizationProvider;
import rseslib.processing.reducts.ReductsProvider;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkState;

/**
 * This handles any size of data your cluster can handle. The only requirement is that each subtable fits into the
 * worker's memory. Currently it supports only object subtables. Supporting attribute subtables will require transposing
 * the data and keeping track of column number - WIP.
 */
public class BigDataAttrSelDriver extends AbstractAttrSelReductsDriver implements Serializable {

    @Override
    public int run(String[] args) throws Exception {
        setUpAttrSelOptions();
        setUpReductsOptions();

        if (parseArguments(args, false, true) == null) {
            return 1;
        }

        // have to get options here not to serialize this
        final boolean dontDiscretize = hasOption("noDiscretize");
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

        JavaRDD<String> txtRows = sc.textFile(getInputPath().toString());

        // matrix rows as RDD
        JavaRDD<double[]> rows = txtRows.map(new Function<String, double[]>() {
            @Override
            public double[] call(String v) throws Exception {
                String[] elts = v.split(",");
                double[] vec = new double[elts.length];
                for (int i = 0; i < elts.length; i++) {
                    vec[i] = Double.parseDouble(elts[i]);
                }
                return vec;
            }
        });

        int numAttrs = rows.first().length - 1;


        final int numSub = getInt("numSubtables");
        int subCard = getInt("subtableCardinality");
        long card = rows.count();

        // this will be the actual number of subtables each row ends in
        final int meanNumberOfSubtablesPerAttribute = (int) (numSub * subCard / card);

        checkState(meanNumberOfSubtablesPerAttribute >= 1);

        // assigns numbers of subtables to rows
        JavaPairRDD<Integer, double[]> subRow = rows.flatMapToPair(
                new PairFlatMapFunction<double[], Integer, double[]>() {
                    @Override
                    public Iterable<Tuple2<Integer, double[]>> call(double[] row) throws Exception {

                        List<Tuple2<Integer, double[]>> result = new ArrayList<>();
                        Random rand = RandomUtils.getRandom();

                        for (int i = 0; i < meanNumberOfSubtablesPerAttribute; i++) {
                            // this is actually bootstrap, since it may draw the same number more than once
                            result.add(new Tuple2<>(rand.nextInt(numSub), row));
                        }

                        return result;
                    }
                }
        );

        // gathers all rows associated with each subtable number
        JavaPairRDD<Integer, Iterable<double[]>> subtables = subRow.groupByKey();

        JavaRDD<List<Integer>> reducts = subtables.flatMap(
                new FlatMapFunction<Tuple2<Integer, Iterable<double[]>>, List<Integer>>() {
                    @Override
                    public Iterable<List<Integer>> call(Tuple2<Integer, Iterable<double[]>> subtableIter) throws Exception {

                        Matrix data =
                                new DenseMatrix(
                                        Iterables.size(subtableIter._2()),
                                        subtableIter._2().iterator().next().length
                                );

                        int i = 0;
                        for (double[] row : subtableIter._2()) {
                            data.set(i, row);
                            i++;
                        }

                        Subtable subtable = new ObjectSubtable(data);

                        RandomReducts randomReducts = dontDiscretize ?
                                // cannot use getRandomReducts, because there are problems serializing AttrSelDriver
                                new RandomReducts(
                                        subtable,
                                        reductsProviderClass,
                                        indiscernibilityForMissing,
                                        discernibilityMethod,
                                        generalizedDecisionTransitiveClosure,
                                        johnsonReducts
                                ) :
                                new RandomReducts(
                                        subtable,
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
                }
        );

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
                                new FrequencyScoreCalculator(val._2(), meanNumberOfSubtablesPerAttribute).getScore()
                        );
                    }
                }
        );

        List<Tuple2<Integer, Double>> result = scores.collect();

        double[] scoresArr = new double[numAttrs];

        for (Tuple2<Integer, Double> tup : result) {
            scoresArr[tup._1()] = tup._2();
        }

        System.out.println("Scores are (<attr>: <score>):");
        for (int i = 0; i < scoresArr.length; i++) {
            System.out.printf("%s: %s%n", i, scoresArr[i]);
        }

        CutoffPointCalculator cutoffCalculator = new FastCutoffPoint(getInt("numCutoffIterations", 1));
        List<Integer> selected = cutoffCalculator.calculateCutoffPoint(Arrays.asList(ArrayUtils.toObject(scoresArr)));

        System.out.printf("Selected attrs: %s%n", selected);
        System.out.printf("Num selected attrs: %s%n", selected.size());

        return 0;
    }

    public static void main(String... args) throws Exception {
        new BigDataAttrSelDriver().run(args);
    }
}
