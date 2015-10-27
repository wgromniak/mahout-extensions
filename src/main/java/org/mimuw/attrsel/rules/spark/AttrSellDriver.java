/*package org.mimuw.attrsel.rules.spark;

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
import org.mimuw.attrsel.common.SerializableMatrix;
import org.mimuw.attrsel.common.SubtableWritable;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.mimuw.attrsel.reducts.AbstractAttrSelReductsDriver;
import org.mimuw.attrsel.reducts.FrequencyScoreCalculator;
import org.mimuw.attrsel.reducts.RandomReducts;
import org.mimuw.attrsel.reducts.RsesDiscretizer;
import org.mimuw.attrsel.reducts.spark.AttrSelDriver;
import rseslib.processing.discretization.ChiMergeDiscretizationProvider;
import rseslib.processing.reducts.ReductsProvider;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class AttrSellDriver extends AbstractAttrSelReductsDriver implements Serializable {

    @Override
    public int run(String[] args) throws Exception {
        setUpAttrSelOptions();
        setUpReductsOptions();

        if (parseArguments(args, false, true) == null) {
            return 1;
        }

        // have to get options here not to serialize this

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);

        SerializableMatrix inputDataTable = readInputMatrix(sc);

        SubtableGenerator<Subtable> subtableGenerator = getSubtableGenerator(inputDataTable.get());
        List<SubtableWritable> subtablesWritable = getWritableSubtables(subtableGenerator);

        JavaRDD<SubtableWritable> distSubtables = sc.parallelize(subtablesWritable);

        // this gives reducts for each subtable

        JavaRDD<List<Integer>> reducts = distSubtables.flatMap(new FlatMapFunction<SubtableWritable, List<Integer>>() {
            @Override
            public Iterable<List<Integer>> call(SubtableWritable subtable) throws Exception {


            });
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
          */