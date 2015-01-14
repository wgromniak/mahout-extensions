package org.mimuw.mahoutattrsel.mapred;

import com.google.common.collect.ImmutableList;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.AttributeSubtable;
import org.mimuw.mahoutattrsel.ObjectSubtable;
import org.mimuw.mahoutattrsel.api.Subtable;
import org.testng.annotations.Test;

import java.io.*;

import static org.mimuw.mahoutattrsel.assertions.AttrselAssertions.assertThat;

public class SubtableWritableTest {

    @Test
    public void testObjectSubtableSerDe() throws Exception {

        Matrix matrix = new DenseMatrix(new double[][]{{1, 2, 3}, {4, 5, 6}, {6, 7, 8}});

        Subtable subtable = new ObjectSubtable(matrix);

        testSerDe(subtable);
    }

    @Test
    public void testAttributeSubtableSerDe() throws Exception {

        Matrix matrix = new DenseMatrix(new double[][]{{1, 2, 3}, {4, 5, 6}, {6, 7, 8}});

        Subtable subtable = new AttributeSubtable(matrix, ImmutableList.of(3, 8), 10);

        testSerDe(subtable);
    }

    private void testSerDe(Subtable original) throws IOException {

        SubtableWritable subtableWritable = new SubtableWritable(original);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutput dataOut = new DataOutputStream(out);

        subtableWritable.write(dataOut);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        DataInput dataIn = new DataInputStream(in);

        Subtable desered = SubtableWritable.read(dataIn).get();

        assertThat(desered.getTable()).isEqualTo(original.getTable());
        assertThat(desered.hasAllAttributes()).isEqualTo(original.hasAllAttributes());
        assertThat(desered.getOriginalNumberOfAttributes()).isEqualTo(original.getOriginalNumberOfAttributes());
        assertThat(desered.getNumberOfAttributes()).isEqualTo(original.getNumberOfAttributes());
        assertThat(desered.iterateAttributes()).containsExactlyElementsOf(original.iterateAttributes());
        assertThat(desered.getAttributeAtPosition(1)).isEqualTo(original.getAttributeAtPosition(1));
    }
}