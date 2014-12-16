package org.mimuw.mahoutattrsel;

import com.google.common.base.Charsets;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.utils.vectors.csv.CSVVectorIterator;
import org.mimuw.mahoutattrsel.api.MatrixReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Reads matrices from CSVs.
 */
public final class CSVMatrixReader implements MatrixReader {

    @Override
    public Matrix read(InputStream is) {

        CSVVectorIterator iterator = new CSVVectorIterator(new InputStreamReader(is, Charsets.UTF_8));

        List<Vector> rows = new ArrayList<>(1000);

        int length;

        if (iterator.hasNext()) {
            Vector vec = iterator.next();
            length = vec.size();
            rows.add(vec);
        } else {
            return new SparseMatrix(0, 0);
        }

        while (iterator.hasNext()) {
            Vector vec = iterator.next();
            checkState(vec.size() == length, "Expected vectors to have same size: %s, but was: %s", length, vec.size());
            rows.add(vec);
        }

        Matrix mat = new DenseMatrix(rows.size(), length);

        for (int i = 0; i < rows.size(); i++) {
            mat.assignRow(i, rows.get(i));
        }

        return mat;
    }

    @Override
    public Matrix read(Path file) {
        try {
            return read(Files.newInputStream(file));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
