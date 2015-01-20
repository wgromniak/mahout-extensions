package org.mimuw.mahoutattrsel.spark;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public final class SerializableMatrix implements Serializable {

    transient private Matrix matrix;

    public Matrix get() {
        return matrix;
    }

    public static SerializableMatrix of(Matrix matrix) {
        SerializableMatrix mat = new SerializableMatrix();
        mat.matrix = matrix;
        return mat;
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();

        MatrixWritable writable = new MatrixWritable();
        writable.readFields(ois);
        matrix = writable.get();
    }

    private void writeObject(ObjectOutputStream ous) throws IOException {
        ous.defaultWriteObject();

        MatrixWritable writable = new MatrixWritable(matrix);
        writable.write(ous);
    }
}
