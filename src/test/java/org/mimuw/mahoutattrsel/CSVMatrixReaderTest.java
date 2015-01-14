package org.mimuw.mahoutattrsel;

import com.google.common.base.Charsets;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.mimuw.mahoutattrsel.api.MatrixReader;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;

import static com.googlecode.catchexception.CatchException.catchException;
import static com.googlecode.catchexception.CatchException.caughtException;
import static org.mimuw.mahoutattrsel.assertions.AttrselAssertions.assertThat;

public class CSVMatrixReaderTest {

    @Test
    public void testNiceMatrixReading() throws Exception {

        String niceMatrixCSV =
                "1, 2.0,3.4\n" +
                "5,3.15,2.72\n" +
                "3,2.1,6.5";

        ByteArrayInputStream in = new ByteArrayInputStream(niceMatrixCSV.getBytes(Charsets.UTF_8));

        MatrixReader reader = new CSVMatrixReader();

        Matrix mat = reader.read(in);

        assertThat(mat)
                .isEqualTo(new DenseMatrix(new double[][]{{1, 2.0, 3.4}, {5, 3.15, 2.72}, {3, 2.1, 6.5}}));
    }

    @Test
    public void testEmptyMatrix() throws Exception {

        String niceMatrixCSV = "";

        ByteArrayInputStream in = new ByteArrayInputStream(niceMatrixCSV.getBytes(Charsets.UTF_8));

        MatrixReader reader = new CSVMatrixReader();

        Matrix mat = reader.read(in);

        assertThat(mat).isEqualTo(new SparseMatrix(0, 0));
    }

    @Test
    public void testWickedMatrix() throws Exception {

        String niceMatrixCSV =
                "1, 2.0,3.4\n" +
                "5,3.15\n" +
                "3,2.1,6.5";

        ByteArrayInputStream in = new ByteArrayInputStream(niceMatrixCSV.getBytes(Charsets.UTF_8));

        MatrixReader reader = new CSVMatrixReader();

        catchException(reader).read(in);

        assertThat(caughtException())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Expected vectors to have same size: 3, but was: 2");
    }
}