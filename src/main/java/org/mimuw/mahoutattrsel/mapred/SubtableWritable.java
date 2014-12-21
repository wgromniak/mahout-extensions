package org.mimuw.mahoutattrsel.mapred;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.mimuw.mahoutattrsel.AttributeSubtable;
import org.mimuw.mahoutattrsel.ObjectSubtable;
import org.mimuw.mahoutattrsel.api.Subtable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public final class SubtableWritable implements Writable {

    private Subtable subtable;

    public SubtableWritable(Subtable subtable) {
        this.subtable = checkNotNull(subtable, "Expected subtable not to be null");
    }

    public SubtableWritable() {}

    public Subtable get() {
        return subtable;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        MatrixWritable matrixWritable = new MatrixWritable(subtable.getTable());
        matrixWritable.write(dataOutput);

        boolean hasAllAttributes = subtable.hasAllAttributes();

        dataOutput.writeBoolean(hasAllAttributes);

        if (!hasAllAttributes) {

            dataOutput.writeInt(subtable.getNumberOfAttributes());

            for (int i : subtable.iterateAttributes()) {
                dataOutput.writeInt(i);
            }

            dataOutput.writeInt(subtable.getOriginalNumberOfAttributes());
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        Matrix table = MatrixWritable.readMatrix(dataInput);

        boolean hasAllAttributes = dataInput.readBoolean();

        if (!hasAllAttributes) {

            int noOfAttributes = dataInput.readInt();

            List<Integer> attributes = new ArrayList<>(noOfAttributes);

            for (int i = 0; i < noOfAttributes; i++) {
                attributes.add(i, dataInput.readInt());
            }

            subtable = new AttributeSubtable(table, attributes, dataInput.readInt());

        } else {
            subtable = new ObjectSubtable(table); // AttributeSubtable with all the attributes will become
                                                  // ObjectSubtable - it's OK, since they are interchangeable in this
                                                  // case
        }
    }

    public static SubtableWritable read(DataInput dataInput) throws IOException {
        SubtableWritable w = new SubtableWritable();
        w.readFields(dataInput);
        return w;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubtableWritable that = (SubtableWritable) o;

        return Objects.equal(this.subtable, that.subtable);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(subtable);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("subtable", subtable)
                .toString();
    }
}
