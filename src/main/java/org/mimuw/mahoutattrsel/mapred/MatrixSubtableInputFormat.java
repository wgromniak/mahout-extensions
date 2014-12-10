package org.mimuw.mahoutattrsel.mapred;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.mimuw.mahoutattrsel.MatrixFixedSizeAttributeSubtableGenerator;
import org.mimuw.mahoutattrsel.MatrixFixedSizeObjectSubtableGenerator;
import org.mimuw.mahoutattrsel.api.Subtable;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * {@link InputFormat} that splits given {@link Matrix} data table into subtables. All the processing is in-memory, so
 * the input table has to fit into single-workstation's memory.
 */
final class MatrixSubtableInputFormat extends InputFormat<IntWritable, MatrixWritable> {

    public static final String SUBTABLE_GENERATOR_TYPE = "mahout-extensions.attrsel.subtable.generator";
    public static final String NO_OF_SUBTABLES = "mahout-extensions.attrsel.number.of.subtables";
    public static final String SUBTABLE_SIZE = "mahout-extensions.attrsel.subtable.size";
    public static final int DEFAULT_NO_OF_SUBTABLES = 1;
    public static final int DEFAULT_SUBTABLE_SIZE = 1;

    public static enum SubtableGeneratorType { ATTRIBUTE, OBJECT }

    private static Optional<Matrix> fullMatrix = Optional.absent();

    /**
     * Loads the input {@link Matrix} data table.
     */
    public static void setFullMatrix(Matrix matrix) {
        checkState(!fullMatrix.isPresent()); // should be set only once
        fullMatrix = Optional.of(checkNotNull(matrix));
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        checkState(fullMatrix.isPresent());

        Configuration conf = jobContext.getConfiguration();

        SubtableGeneratorType generatorType = conf.getEnum(SUBTABLE_GENERATOR_TYPE, SubtableGeneratorType.OBJECT);

        SubtableGenerator<Matrix> subtableGenerator;

        int numberOfSubtables = conf.getInt(NO_OF_SUBTABLES, DEFAULT_NO_OF_SUBTABLES);
        int subtableSize = conf.getInt(SUBTABLE_SIZE, DEFAULT_SUBTABLE_SIZE);

        switch (generatorType) {
            case ATTRIBUTE:
                subtableGenerator = new MatrixFixedSizeAttributeSubtableGenerator(RandomUtils.getRandom(),
                        numberOfSubtables, subtableSize, fullMatrix.get());
                break;
            case OBJECT:
                subtableGenerator = new MatrixFixedSizeObjectSubtableGenerator(RandomUtils.getRandom(),
                        numberOfSubtables, subtableSize, fullMatrix.get());
                break;
            default:
                throw new IllegalStateException(String.format("Unsupported generator type: %s", generatorType));
        }

        List<Subtable> subtables = subtableGenerator.getSubtables();

        List<InputSplit> splits = new ArrayList<>(subtables.size());

        for (int i = 0; i < subtables.size(); i++) {
//            splits.add(new SingleMatrixInputSplit(i, subtables.get(i)));
        }

        return splits;
    }

    @Override
    public RecordReader<IntWritable, MatrixWritable> createRecordReader(InputSplit inputSplit,
                                       TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new SingleMatrixRecordReader();
    }

    /**
     * A single InputSplit corresponds to single {@link Matrix} subtable.
     */
    static final class SingleMatrixInputSplit extends InputSplit {

        private MatrixWritable matrix;
        private int key;

        private SingleMatrixInputSplit() {}

        private SingleMatrixInputSplit(int key, Matrix matrix) {
            this.key = key;
            this.matrix = new MatrixWritable(checkNotNull(matrix));
        }

        @Override
        public long getLength() {
            return matrix.get().rowSize();
        }

        @Override
        public String[] getLocations() {
            return new String[]{}; // for, it's in-memory
        }
    }

    /**
     * A single record corresponds to single {@link Matrix} subtable from {@link SingleMatrixInputSplit}.
     */
    static final class SingleMatrixRecordReader extends RecordReader<IntWritable, MatrixWritable> {

        private int key;
        private MatrixWritable matrix;

        private boolean processed = false; // whether this record has been processed

        SingleMatrixRecordReader() {}

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {

            SingleMatrixInputSplit matrixSplit = ((SingleMatrixInputSplit) split);

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
        public MatrixWritable getCurrentValue() {
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
