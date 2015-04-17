package org.mimuw.attrsel;

import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVStrategy;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * TODO: this does not work and does not have any tests
 */
public class DataGenerator {

    private int numberOfAttributes;
    private int numberOfRows;

    public DataGenerator(int attrCount, int rowsCount)
    {
        checkArgument(attrCount > 0, "Attribute number must be positive");
        checkArgument(rowsCount > 0, "Rows number must be positive");

        numberOfAttributes = attrCount;
        numberOfRows = rowsCount;
    }

    public void writeToCSV(Matrix data) throws IOException {
        String csv = System.getProperty("user.dir") + "Data.csv";

        CSVPrinter writer = new CSVPrinter(new FileWriter(csv), CSVStrategy.DEFAULT_STRATEGY);

        for (int i = 0; i < numberOfRows; i++)
        {
            Vector row = data.viewRow(i);
            String rowToCSV[] = new String[row.size()];
            for (int j = 0; j < row.size(); j++)
            {
                rowToCSV[j] = Double.toString(row.get(j));
            }
            writer.println(rowToCSV);
        }
    }

    public Matrix generateData(int numberOfCorrelatedAttributes) {
        checkArgument(numberOfAttributes >= numberOfCorrelatedAttributes,
                "number of significant attributes must not be greater than number of attributes!");
        Matrix data = new DenseMatrix(numberOfRows, numberOfAttributes + 1);
        for (int i = 0; i < numberOfRows; i++) {
            data.assignRow(i, generateRow(numberOfAttributes + 1, numberOfCorrelatedAttributes));
        }
        return data;
    }

    private Vector generateRow(int vectorSize, int numberOfCorrelatedAttributes)
    {
        Vector randomVector = new DenseVector(vectorSize);
        Random random = new Random();
        Double number;
        int base;
        double VARIANCE = 5.0f;
        for ( int i = 0; i < (vectorSize-1); i++ )
        {
            if ( i < numberOfCorrelatedAttributes ) {
                base = random.nextInt(numberOfAttributes / 2);
                number = (double) base + random.nextGaussian() * VARIANCE;
            }
            else {
                base = random.nextInt(numberOfAttributes);
                number = (double) base + random.nextGaussian();
            }
            randomVector.set(i, number);
        }
        randomVector.set(vectorSize, correlationFunction(randomVector, numberOfCorrelatedAttributes));
        return randomVector;
    }

    private int correlationFunction(Vector vectorOfData, int numberOfCorrelatedAttributes)
    {
        double sum = 0;
        for ( int i = 0; i < numberOfCorrelatedAttributes; i++ )
        {
            sum += vectorOfData.get(i);
        }
        sum = sum/numberOfCorrelatedAttributes;
        if (sum > (double) numberOfAttributes/4) {
            return 1;
        }
        else {
            return 0;
        }
    }
}
