package org.mimuw.mahoutattrsel.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class MatrixSubtableInputFormatTest {

    @Test
    public void testJobInputCreation() throws Exception {

        Random random = new Random();
        Matrix fullMat = new DenseMatrix(20, 9);

        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 9; j++) {
                fullMat.setQuick(i, j, random.nextDouble());
            }
        }

        MatrixSubtableInputFormat.setFullMatrix(fullMat);

        MatrixSubtableInputFormat inputFormat = new MatrixSubtableInputFormat();

        Configuration conf = new Configuration();
        conf.setEnum(MatrixSubtableInputFormat.SUBTABLE_GENERATOR_TYPE,
                MatrixSubtableInputFormat.SubtableGeneratorType.OBJECT);
        conf.setInt(MatrixSubtableInputFormat.NO_OF_SUBTABLES, 3);
        conf.setInt(MatrixSubtableInputFormat.SUBTABLE_SIZE, 10);

        List<InputSplit> splits = inputFormat.getSplits(new JobContext(conf, JobID.forName("job_1_2")));

        assertThat(splits).hasSize(3);
        assertThat(splits.get(0).getLength()).isEqualTo(10);

        MatrixSubtableInputFormat.SingleMatrixRecordReader reader =
                new MatrixSubtableInputFormat.SingleMatrixRecordReader();

        reader.initialize(splits.get(1), mock(TaskAttemptContext.class));

        assertThat(reader.getCurrentKey()).isNull();
        assertThat(reader.getProgress()).isEqualTo(0f);
        assertThat(reader.nextKeyValue()).isTrue();
        assertThat(reader.getProgress()).isEqualTo(1f);
        assertThat(reader.getCurrentKey()).isEqualTo(new IntWritable(1));
        assertThat(reader.getCurrentValue().get().rowSize()).isEqualTo(10);
        assertThat(reader.getCurrentValue().get().columnSize()).isEqualTo(9);
        assertThat(reader.nextKeyValue()).isFalse();
    }
}