package org.mimuw.attrsel.trees.spark;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.mimuw.attrsel.common.CSVMatrixReader;
import org.mimuw.attrsel.common.SerializableMatrix;
import org.mimuw.attrsel.common.SubtableWritable;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.mimuw.attrsel.trees.AbstractAttrSelTreesDriver;
import org.mimuw.attrsel.trees.MCFS;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class TreeAttrSelDriver extends AbstractAttrSelTreesDriver implements Serializable {

    @Override
    public int run(String[] args) throws Exception {

        setUpAttrSelOptions();
        setUpTreesOptions();

        if (parseArguments(args, false, true) == null) {
            return 1;
        }

        // have to get options here not to serialize this
        final int numTrees = getInt("numTrees", 100);
        final Long seed = Long.valueOf(getOption("seed"));
        final Double u = Double.valueOf(getOption("u", "2"));
        final Double v = Double.valueOf(getOption("v", "2"));

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);

        SerializableMatrix inputDataTable = readInputMatrix(sc);

        SubtableGenerator<Subtable> subtableGenerator = getSubtableGenerator(inputDataTable.get());
        final List<SubtableWritable> subtablesWritable = getWritableSubtables(subtableGenerator);


        // broadcast number of sub per attr
        List<Integer> tmpNumSubPerAttr = subtableGenerator.getNumberOfSubtablesPerAttribute();
        tmpNumSubPerAttr = new ArrayList<>(tmpNumSubPerAttr); // tmpNumSubPerAttr is not Serializable
        final Broadcast<List<Integer>> numSubPerAttr = sc.broadcast(tmpNumSubPerAttr);

        JavaRDD<SubtableWritable> distSubtables = sc.parallelize(subtablesWritable);



        JavaPairRDD<Integer, Double> mapResult = distSubtables.flatMapToPair(
                new PairFlatMapFunction<SubtableWritable, Integer, Double>() {
                    @Override
                    public Iterable<Tuple2<Integer, Double>> call(SubtableWritable subtableWritable) throws Exception {

                        MCFS mcfs = new MCFS(numTrees, seed, u, v);

                        double[] scores = mcfs.getScores(subtableWritable.get().getTable());
                        List<Tuple2<Integer, Double>> result = new ArrayList<>(scores.length);

                        for (int i = 0; i < scores.length; i++) {
                            result.add(i, new Tuple2<>(subtableWritable.get().getAttributeAtPosition(i), scores[i]));
                        }

                        return result;
                    }
                }
        );

        JavaPairRDD<Integer, Double> scores = mapResult.reduceByKey(
                new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double v1, Double v2) throws Exception {
                        return v1 + v2;
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
        new TreeAttrSelDriver().run(args);
    }
}
