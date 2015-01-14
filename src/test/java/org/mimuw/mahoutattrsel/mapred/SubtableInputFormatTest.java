package org.mimuw.mahoutattrsel.mapred;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.math.DenseMatrix;
import org.mimuw.mahoutattrsel.ObjectSubtable;
import org.mimuw.mahoutattrsel.api.Subtable;
import org.testng.annotations.Test;

import java.util.List;

import static org.mimuw.mahoutattrsel.assertions.AttrselAssertions.assertThat;
import static org.mockito.Mockito.mock;

public class SubtableInputFormatTest {

    @Test
    public void testJobInputCreation() throws Exception {

        List<Subtable> subtables
                = ImmutableList.<Subtable>of(new ObjectSubtable(new DenseMatrix(new double[][]{{1}})),
                                             new ObjectSubtable(new DenseMatrix(new double[][]{{1, 2}, {3, 4}})),
                                             new ObjectSubtable(new DenseMatrix(new double[][]{{3}, {5}})));

        SubtableInputFormat.setSubtables(subtables);

        SubtableInputFormat inputFormat = new SubtableInputFormat();
        List<InputSplit> splits = inputFormat.getSplits(Job.getInstance(new Configuration(), "jooob"));

        assertThat(splits).hasSize(3);
        assertThat(splits.get(1).getLength()).isEqualTo(2);

        SubtableInputFormat.SingleSubtableRecordReader reader =
                new SubtableInputFormat.SingleSubtableRecordReader();

        reader.initialize(splits.get(1), mock(TaskAttemptContext.class));

        assertThat(reader.getCurrentKey()).isNull();
        assertThat(reader.getProgress()).isEqualTo(0f);
        assertThat(reader.nextKeyValue()).isTrue();
        assertThat(reader.getProgress()).isEqualTo(1f);
        assertThat(reader.getCurrentKey()).isEqualTo(new IntWritable(1));
        assertThat(reader.getCurrentValue().get().getTable().rowSize()).isEqualTo(2);
        assertThat(reader.getCurrentValue().get().getTable().columnSize()).isEqualTo(2);
        assertThat(reader.nextKeyValue()).isFalse();
    }
}