package org.mimuw.attrsel.reducts.mapred;

import com.google.common.base.Objects;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link Writable} of a {@link BitSet}.
 */
public final class BitSetWritable implements Writable {

    private BitSet set;

    public BitSetWritable() {}

    public BitSetWritable(BitSet set) {
        this.set = checkNotNull(set, "Expected the set not to be null");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        long[] setArray = set.toLongArray();
        dataOutput.writeInt(setArray.length);

        for (long el : setArray) {
            dataOutput.writeLong(el);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        long[] setArray = new long[dataInput.readInt()];

        for (int i = 0; i < setArray.length; i++) {
            setArray[i] = dataInput.readLong();
        }

        set = BitSet.valueOf(setArray);
    }

    public static BitSetWritable read(DataInput dataInput) throws IOException {
        BitSetWritable w = new BitSetWritable();
        w.readFields(dataInput);
        return w;
    }

    public BitSet get() {
        return set;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BitSetWritable that = (BitSetWritable) o;

        return Objects.equal(this.set, that.set);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(set);
    }


    @Override
    public String toString() {
        return set.toString();
    }
}
