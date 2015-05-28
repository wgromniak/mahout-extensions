package org.mimuw.attrsel.common.spark;

import com.google.common.collect.ImmutableList;
import org.apache.mahout.math.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import weka.datagenerators.classifiers.classification.RDG1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * To generate a GB of data run with args: 512 32 32768.
 */
public class DataGen {

    public static void main(final String... args)  {

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);

        final int numAttrs = Integer.parseInt(args[0]);
        final int numPartitions = Integer.parseInt(args[1]);
        final int numObjPerPartition = Integer.parseInt(args[2]);

        // this will create the data only in the executors
        sc.parallelize(ImmutableList.of(), numPartitions).mapPartitions(new FlatMapFunction<Iterator<Object>, String>() {
            @Override
            public Iterable<String> call(Iterator<Object> objectIterator) throws Exception {

                RDG1 dg = new RDG1();
                dg.setNumAttributes(numAttrs);
                dg.defineDataFormat();
                dg.setNumIrrelevant(numAttrs - 10);

                int[] relevant = new int[10];
                for (int i = 0, j = 0; i < dg.getAttList_Irr().length; i++) {
                    if (!dg.getAttList_Irr()[i]) {
                        relevant[j++] = i;
                    }
                }

                System.out.println("Relevant attribute numbers: " + Arrays.toString(relevant));

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
