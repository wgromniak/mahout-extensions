package org.mimuw.attrsel.reducts.mapred;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

final class IntListWritable implements Writable {

    private List<Integer> list;

    public IntListWritable(List<Integer> list) {
        this.list = checkNotNull(list, "Expected list not to be null");
    }

    public IntListWritable() {}

    public List<Integer> get() {
        return list;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(list.size());
        for (int i : list) {
            dataOutput.writeInt(i);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        int size = dataInput.readInt();

        List<Integer> list = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            list.add(i, dataInput.readInt());
        }

        this.list = list;
    }

    public static IntListWritable read(DataInput dataInput) throws IOException{
        IntListWritable w = new IntListWritable();
        w.readFields(dataInput);
        return w;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntListWritable that = (IntListWritable) o;

        return Objects.equal(this.list, that.list);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(list);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("list", list)
                .toString();
    }
}
