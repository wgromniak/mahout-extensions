package org.mimuw.mahoutattrsel.mapred;

import org.testng.annotations.Test;

import java.io.*;
import java.util.BitSet;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class BitSetWritableTest {

    @Test
    public void testSerialisationDeserialisation() throws Exception {

        Random rnd = new Random(12345);

        byte[] randomBytes = new byte[1000];
        rnd.nextBytes(randomBytes);

        BitSet expectedSet = BitSet.valueOf(randomBytes);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutput dataOut = new DataOutputStream(out);

        new BitSetWritable(expectedSet).write(dataOut);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        DataInput dataIn = new DataInputStream(in);

        BitSet actualSet = BitSetWritable.read(dataIn).get();

        assertThat(actualSet).isEqualTo(expectedSet);
    }
}