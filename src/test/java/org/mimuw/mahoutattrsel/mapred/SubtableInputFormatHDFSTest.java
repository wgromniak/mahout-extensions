package org.mimuw.mahoutattrsel.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.mimuw.mahoutattrsel.MatrixFixedSizeObjectSubtableGenerator;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;
import org.testng.annotations.*;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class SubtableInputFormatHDFSTest {

    private Path tempPath;
    private MiniDFSCluster dfsCluster;

    @BeforeClass
    public void startDFSCluster() throws Exception {
        dfsCluster = new MiniDFSCluster.Builder(new Configuration())
                .numDataNodes(3)
                .build();

        dfsCluster.waitActive();
    }

    @AfterClass
    public void stopDFSCluster() {
        dfsCluster.shutdown();
    }

    @BeforeMethod
    public void setUp() throws IOException {
        tempPath = new Path("hdfs:///mahout-extensions/attrsel");
        dfsCluster.getFileSystem().mkdirs(tempPath);

        SubtableInputFormat.setFileSystem(dfsCluster.getFileSystem());
    }

    @AfterMethod
    public void deleteTempDirectory() throws IOException {
        dfsCluster.getFileSystem().delete(tempPath, true);
    }

    @Test
    public void testJobInputCreation() throws Exception {

        Matrix fullMat = new DenseMatrix(20, 9);

        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 9; j++) {
                fullMat.setQuick(i, j, 53);
            }
        }

        SubtableInputFormat.setDataTable(fullMat);

        SubtableInputFormat inputFormat = new SubtableInputFormat();

        Configuration conf = new Configuration();
        conf.setClass(SubtableInputFormat.SUBTABLE_GEN,
                MatrixFixedSizeObjectSubtableGenerator.class, SubtableGenerator.class);
        conf.setInt(SubtableInputFormat.NUM_SUBTABLES, 3);
        conf.setInt(SubtableInputFormat.SUBTABLE_CARD, 10);

        List<InputSplit> splits = inputFormat.getSplits(Job.getInstance(conf, "jooob"));

        assertThat(splits).hasSize(3);
        assertThat(splits.get(0).getLength()).isEqualTo(10);

        SubtableInputFormat.SingleSubtableRecordReader reader =
                new SubtableInputFormat.SingleSubtableRecordReader();

        reader.initialize(splits.get(1), mock(TaskAttemptContext.class));

        assertThat(reader.getCurrentKey()).isNull();
        assertThat(reader.getProgress()).isEqualTo(0f);
        assertThat(reader.nextKeyValue()).isTrue();
        assertThat(reader.getProgress()).isEqualTo(1f);
        assertThat(reader.getCurrentKey()).isEqualTo(new IntWritable(1));
        assertThat(reader.getCurrentValue().get().getTable().rowSize()).isEqualTo(10);
        assertThat(reader.getCurrentValue().get().getTable().columnSize()).isEqualTo(9);
        assertThat(reader.nextKeyValue()).isFalse();

        FileSystem fs = dfsCluster.getFileSystem();
        Path attrsPath = new Path(SubtableInputFormat.NUM_SUBTABLES_ATTRIBUTE_PATH);
        DataInput in = fs.open(attrsPath);

        assertThat(fs.exists(attrsPath)).isTrue();
        List<Integer> attrCounts = IntListWritable.read(in).get();
        assertThat(attrCounts).hasSize(8);
    }
}