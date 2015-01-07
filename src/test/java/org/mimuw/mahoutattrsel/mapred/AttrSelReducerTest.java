
package org.mimuw.mahoutattrsel.mapred;



import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.annotations.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


public class AttrSelReducerTest {


    private static final String NUM_SUBTABLE_ATTRIBUTE_PATH =
            "file:///home/kris/DRACUS/mahout-extensions/numSubAttrs";


    @Test
    public void testScore() throws Exception {
        AttrSelReducer reducer = new AttrSelReducer();
        ReduceDriver<IntWritable, IntListWritable, IntWritable, DoubleWritable> reduceDriver =
                ReduceDriver.newReduceDriver(reducer);

        // writing list to file
        List<Integer> numberOfSubtablePerAttribute = Arrays.asList(3, 4, 5, 6);
        IntListWritable numberOfSubtablePerAttributeWritable = new IntListWritable(numberOfSubtablePerAttribute);
        Path path = new Path(NUM_SUBTABLE_ATTRIBUTE_PATH);
        FileOutputStream fos = new FileOutputStream(new File(path.toUri()));
        DataOutput dos = new DataOutputStream(fos);
        numberOfSubtablePerAttributeWritable.write(dos);

        ArrayList<IntListWritable> reducts = new ArrayList<>();
        reducts.add(new IntListWritable(Arrays.asList(1, 3, 4)));
        reducts.add(new IntListWritable(Arrays.asList(1, 3, 4)));
        reducts.add(new IntListWritable(Arrays.asList(1, 3, 4)));


        int attrKey1 = 1;
        DoubleWritable score1 = new DoubleWritable(0.75);
        int attrKey2 = 2;
        DoubleWritable score2 = new DoubleWritable(0.6);

        reduceDriver.withCacheFile(path.toUri())
                .withInput(new IntWritable(attrKey1), reducts)
                .withOutput(new IntWritable(attrKey1), score1)
                .withInput(new IntWritable(attrKey2), reducts)
                .withOutput(new IntWritable(attrKey2), score2)
                .runTest();
    }

    @Test
    public void testFileWriting() throws Exception {

        List<Integer> numberOfSubtablePerAttribute = Arrays.asList(3, 4, 5, 6);
        IntListWritable numberOfSubtablePerAttributeWritable = new IntListWritable(numberOfSubtablePerAttribute);
        Path path = new Path(NUM_SUBTABLE_ATTRIBUTE_PATH);
        FileOutputStream fos = new FileOutputStream(new File(path.toUri()));
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        numberOfSubtablePerAttributeWritable.write(oos);
        oos.close();


        FileInputStream fis = new FileInputStream(new File(path.toUri()));
        ObjectInputStream ois = new ObjectInputStream(fis);
        List<Integer> list = IntListWritable.read(ois).get();
        ois.close();

        assertThat(list).isEqualTo(numberOfSubtablePerAttribute);
    }

}