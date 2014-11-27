package org.mimuw.mahoutattrsel;

import com.google.common.collect.ImmutableList;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.mimuw.mahoutattrsel.api.SubtableGenerator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

public final class MatrixFixedSizeObjectSubtableGenerator implements SubtableGenerator<Matrix> {

    private final Random random;
    private final int numberOfSubtables;
    private final int subtableSize;
    
    private final Matrix dataTable;

    /**
     * It's constructor for this class. 
     * @param random
     * @param numberOfSubtables
     * @param subtableSize
     */
    public MatrixFixedSizeObjectSubtableGenerator(Random random, int numberOfSubtables, int subtableSize, Matrix dataTable) {
        
        checkArgument(numberOfSubtables > 0);
        checkArgument(subtableSize > 0);
        
        this.random = random;
        this.numberOfSubtables = numberOfSubtables;
        this.subtableSize = subtableSize;
        this.dataTable = dataTable;
    }

    @Override
    public List<Matrix> getSubtables(){

        ImmutableList.Builder<Matrix> resultBuilder = ImmutableList.builder();

        int numberOfObjects = dataTable.columnSize(); // number of objects

        for(int i = 0; i < numberOfSubtables; i++){

            BitSet selectedObjects = drawObjects(numberOfObjects, subtableSize);

            ArrayList<Vector> subtable = new ArrayList<>();

            for(int rowNum = 0; rowNum < numberOfObjects; rowNum++){

                if(selectedObjects.get(rowNum)){

                    subtable.add(dataTable.viewRow(rowNum));
                }
            }

            resultBuilder.add(convertToMatrix(subtable));
        }
        return resultBuilder.build();
    }

    Matrix convertToMatrix(ArrayList<Vector> subtable) {
        Matrix matrix = new DenseMatrix(subtable.size(),dataTable.rowSize());
        int i = 0;
        for(Vector v: subtable){
            matrix.assignRow(i, v);
            i++;
        }
        return  matrix;
    }

    private BitSet drawObjects(int numberOfObjects, int sizeOfSubtable){

        BitSet selected = new BitSet(numberOfObjects);

        while(sizeOfSubtable > 0 ){

            int next = random.nextInt(numberOfObjects);

            if(!selected.get(next)){

                selected.set(next);
                sizeOfSubtable--;
            }
        }

        return selected;
    }



}
