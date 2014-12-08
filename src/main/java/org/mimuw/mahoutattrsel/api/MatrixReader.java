package org.mimuw.mahoutattrsel.api;

import org.apache.mahout.math.Matrix;

import java.io.InputStream;
import java.nio.file.Path;

public interface MatrixReader {

    Matrix read(InputStream is);

    Matrix read(Path file);
}
