package org.mimuw.mahoutattrsel.mapred;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.MatrixFixedSizeObjectSubtableGenerator;
import org.mimuw.mahoutattrsel.api.Subtable;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * {@link InputFormat} that splits given {@link Matrix} data table into subtables. All the processing is in-memory, so
 * the input table has to fit into single-workstation's memory.
 */
final class SubtableInputFormat extends InputFormat<IntWritable, SubtableWritable> {

    public static final String SUBTABLE_GENERATOR_TYPE = "mahout-extensions.attrsel.subtable.generator";
    public static final String NO_OF_SUBTABLES = "mahout-extensions.attrsel.number.of.subtables";
    public static final String SUBTABLE_SIZE = "mahout-extensions.attrsel.subtable.size";
    public static final int DEFAULT_NO_OF_SUBTABLES = 1;
    public static final int DEFAULT_SUBTABLE_SIZE = 1;

    static final String NUM_SUBTABLE_ATTRIBUTE_PATH = "hdfs:///mahout-extensions/attrsel/numSubAttrs";

    private static Optional<Matrix> fullMatrix = Optional.absent();
    private static Optional<FileSystem> fs = Optional.absent();

    /**
     * Loads the input {@link Matrix} data table.
     */
    public static void setFullMatrix(Matrix matrix) {
        checkState(!fullMatrix.isPresent()); // should be set only once
        fullMatrix = Optional.of(checkNotNull(matrix));
    }

    public static void setFileSystem(FileSystem fileSystem) {
        checkState(!fs.isPresent());
        fs = Optional.of(fileSystem);
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        checkState(fullMatrix.isPresent());
        checkState(fs.isPresent());

        Configuration conf = jobContext.getConfiguration();

        @SuppressWarnings("unchecked")
        Class<SubtableGenerator<Subtable>> generatorClass =
                (Class<SubtableGenerator<Subtable>>) conf.getClass(SUBTABLE_GENERATOR_TYPE,
                        MatrixFixedSizeObjectSubtableGenerator.class);

        int numberOfSubtables = conf.getInt(NO_OF_SUBTABLES, DEFAULT_NO_OF_SUBTABLES);
        int subtableSize = conf.getInt(SUBTABLE_SIZE, DEFAULT_SUBTABLE_SIZE);

        SubtableGenerator<Subtable> subtableGenerator;

        try {
            subtableGenerator = generatorClass
                    .getConstructor(Random.class, int.class, int.class, Matrix.class)
                    .newInstance(RandomUtils.getRandom(), numberOfSubtables, subtableSize, fullMatrix.get());

        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalStateException("Error instantiating SubtableGenerator", e);
        }

        List<Subtable> subtables = subtableGenerator.getSubtables();

        List<InputSplit> splits = new ArrayList<>(subtables.size());

        for (int i = 0; i < subtables.size(); i++) {
            splits.add(new SingleSubtableInputSplit(i, subtables.get(i)));
        }

        List<Integer> numberOfSubtablesPerAttribute = subtableGenerator.getNumberOfSubtablesPerAttribute();

        writeAttributeCountsToHDFSAndSetCache(numberOfSubtablesPerAttribute, jobContext);

        return splits;
    }

    // TODO: this will be moved to job config
    private void writeAttributeCountsToHDFSAndSetCache(List<Integer> numberOfSubtablesPerAttribute,
                                                       JobContext jobContext) throws IOException {
        Path path = new Path(NUM_SUBTABLE_ATTRIBUTE_PATH);

        try (FSDataOutputStream os = fs.get().create(path, true)) {

            new IntListWritable(numberOfSubtablesPerAttribute).write(os);
        }

        // TODO: this will be eventually done elsewhere, using non-deprecated means
        DistributedCache.addCacheFile(path.toUri(), jobContext.getConfiguration());
    }

    @Override
    public RecordReader<IntWritable, SubtableWritable> createRecordReader(InputSplit inputSplit,
                                       TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new SingleSubtableRecordReader();
    }

    /**
     * A single InputSplit corresponds to single {@link Subtable}.
     */
    static final class SingleSubtableInputSplit extends InputSplit {

        private SubtableWritable matrix;
        private int key;

        private SingleSubtableInputSplit() {}

        private SingleSubtableInputSplit(int key, Subtable matrix) {
            this.key = key;
            this.matrix = new SubtableWritable(checkNotNull(matrix));
        }

        @Override
        public long getLength() {
            return matrix.get().getTable().rowSize();
        }

        @Override
        public String[] getLocations() {
            return new String[]{}; // for, it's in-memory
        }
    }

    /**
     * A single record corresponds to single {@link Subtable} from {@link SingleSubtableInputSplit}.
     */
    static final class SingleSubtableRecordReader extends RecordReader<IntWritable, SubtableWritable> {

        private int key;
        private SubtableWritable matrix;

        private boolean processed = false; // whether this record has been processed

        SingleSubtableRecordReader() {}

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
