package org.mimuw.mahoutattrsel;

import com.google.common.collect.ImmutableList;
import org.apache.mahout.math.Matrix;
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
        List<Matrix> dataObjects = dataTable; //It's problem

        int numberOfObjects = dataTable.rowSize();

        for(int i = 0; i < numberOfSubtables; i++){

            BitSet selectedObjects = drawObjects(numberOfObjects, subtableSize);

            ArrayList<Matrix> subtable = new ArrayList<Matrix>(dataTable.) //second problem

            for(int j = 0; j < numberOfObjects; j++){

                if(selectedObjects.get(j)){

                    subtable.add(dataObjects.get(j));
                }
            }

            resultBuilder.add(subtable);
        }
        return resultBuilder.build();
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
