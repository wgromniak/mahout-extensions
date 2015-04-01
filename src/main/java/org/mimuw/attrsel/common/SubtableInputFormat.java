package org.mimuw.attrsel.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.mahout.math.Matrix;
import org.mimuw.attrsel.common.api.Subtable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * {@link InputFormat} that splits given {@link Matrix} data table into subtables. All the processing is in-memory, so
 * the input table has to fit into single-workstation's memory.
 */
public final class SubtableInputFormat extends InputFormat<IntWritable, SubtableWritable> {

    private static Optional<List<Subtable>> subtables = Optional.absent();

    public static void setSubtables(List<Subtable> subtables) {
        checkState(!SubtableInputFormat.subtables.isPresent(), "Expected subtables to be absent");
        SubtableInputFormat.subtables = Optional.of(subtables);
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        checkState(subtables.isPresent(), "Expected subtables to be present");

        List<InputSplit> splits = new ArrayList<>(subtables.get().size());

        for (int i = 0; i < subtables.get().size(); i++) {
            splits.add(new SingleSubtableInputSplit(i, subtables.get().get(i)));
        }

        return splits;
    }

    @Override
    public RecordReader<IntWritable, SubtableWritable> createRecordReader(InputSplit inputSplit,
                                       TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new SingleSubtableRecordReader();
    }

    /**
     * A single InputSplit corresponds to single {@link Subtable}.
     */
    static final class SingleSubtableInputSplit extends InputSplit implements Writable {

        private SubtableWritable matrix;
        private int key;

        private SingleSubtableInputSplit() {} // used by reflection

        private SingleSubtableInputSplit(int key, Subtable matrix) {
            this.key = key;
            this.matrix = new SubtableWritable(checkNotNull(matrix, "Expected matrix not to be null"));
        }

        @Override
        public long getLength() {
            return matrix.get().getTable().rowSize();
        }

        @Override
        public String[] getLocations() {
            return new String[]{}; // for, it's in-memory
        }

        @Override
        public void write(DataOutput out) throws IOException {
            matrix.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {

            if (matrix == null) {
                matrix = new SubtableWritable();
            }

            matrix.readFields(in);
        }
    }

    /**
     * A single record corresponds to single {@link Subtable} from {@link SingleSubtableInputSplit}.
     */
    @VisibleForTesting
    public static final class SingleSubtableRecordReader extends RecordReader<IntWritable, SubtableWritable> {

        private int key;
        private SubtableWritable matrix;

        private boolean processed = false; // whether this record has been processed

        @VisibleForTesting
        public SingleSubtableRecordReader() {}

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {

            SingleSubtableInputSplit matrixSplit = ((SingleSubtableInputSplit) split);

            key = matrixSplit.key;
            matrix = matrixSplit.matrix;

            processed = false;
        }

        @Override
        public boolean nextKeyValue() {
            if (!processed) {
                processed = true;

                return true;
            }
            return false;
        }

        @Override
        public IntWritable getCurrentKey() {
            if (processed) {
                return new IntWritable(key);
            } else {
                return null;
            }
        }

        @Override
        public SubtableWritable getCurrentValue() {
            return matrix;
        }

        @Override
        public float getProgress() {
            if (processed) {
                return 1f;
            } else {
                return 0;
            }
        }

        @Override
        public void close() {}
    }
}
