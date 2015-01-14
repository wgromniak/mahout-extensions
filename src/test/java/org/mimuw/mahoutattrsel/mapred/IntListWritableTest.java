package org.mimuw.mahoutattrsel.mapred;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.*;
import java.util.List;

import static org.mimuw.mahoutattrsel.assertions.AttrselAssertions.assertThat;

public class IntListWritableTest {

    @Test
    public void test() throws Exception {

        List<Integer> list = ImmutableList.of(2, 3, 2, 5);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutput dataOut = new DataOutputStream(out);

        new IntListWritable(list).write(dataOut);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        DataInput dataIn = new DataInputStream(in);

        List<Integer> actualList = IntListWritable.read(dataIn).get();

        assertThat(actualList).isEqualTo(list);
    }
}