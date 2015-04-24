package org.mimuw.attrsel.common.spark;

import com.google.common.collect.ImmutableList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import weka.datagenerators.classifiers.classification.RDG1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * To generate a TB of data run with args: 512 32 32768.
 */
public class DataGen {

    public static void main(final String... args)  {

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);

        final int numAttrs = Integer.parseInt(args[0]);
        final int numPartitions = Integer.parseInt(args[1]);
        final int numObjPerPartition = Integer.parseInt(args[2]);

        sc.parallelize(ImmutableList.of(), numPartitions).mapPartitions(new FlatMapFunction<Iterator<Object>, String>() {
            @Override
            public Iterable<String> call(Iterator<Object> objectIterator) throws Exception {

                RDG1 dg = new RDG1();
                dg.setNumAttributes(numAttrs);
                dg.defineDataFormat();
                dg.setNumIrrelevant(numAttrs - 10);

                List<String> toBeSaved = new ArrayList<>(numObjPerPartition);

                for (int i = 0; i < numObjPerPartition; i++) {
                    toBeSaved.add(
                            dg.generateExample().toString()
                                    .replace("true", "1")
                                    .replace("false", "0")
                                    .replace("c0", "0")
                                    .replace("c1", "1")
                    );
                }

                return toBeSaved;
            }
        }).saveAsTextFile(args[3]);
    }
}
