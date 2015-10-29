package org.mimuw.attrsel.rules;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public class DistinguishMatrixGenerator {

    private final Matrix inputDataMatrix;
    private Set<Integer>[][] distinguishTable;
    private final int numberOfObjects;
    private ArrayList<Integer> listOfAttributes;
    //private ArrayList<Boolean> coveredObject;

    @SuppressWarnings("unchecked")
    public DistinguishMatrixGenerator(Matrix inputDataMatrix) {

        checkNotNull(inputDataMatrix);
        checkArgument(inputDataMatrix.rowSize() > 0);
        checkArgument(inputDataMatrix.columnSize() > 0);
        this.inputDataMatrix = inputDataMatrix;
        this.numberOfObjects = this.inputDataMatrix.rowSize();
        this.distinguishTable = new Set[numberOfObjects][numberOfObjects];
        this.listOfAttributes = new ArrayList<>();
      //  this.coveredObject = new ArrayList<>();
    }

    /*
     1) Computing distinguish matrix for all table
     2) Get one column of this distinguish matrix
     */

    public Set<Integer>[][] computeDistinguishMatrix() {
        for (int i = 0; i < numberOfObjects; i++) {
            for (int j = 0; j < numberOfObjects; j++) {
                distinguishTable[i][j] = new HashSet<>();
            }
        }

        for (MatrixSlice firstObject : this.inputDataMatrix) {
            for (MatrixSlice secondObject : this.inputDataMatrix)
                if ((firstObject.get(firstObject.size() - 1)) != (secondObject.get(secondObject.size() - 1))) {
                    int count = firstObject.size() - 2;//I don't want decision
                    ArrayList<Integer> put = new ArrayList<>();
                    while (count >= 0) {
                        if (firstObject.get(count) != secondObject.get(count)) {
                            put.add(count);
                            this.listOfAttributes.add(count);
                        }
                        count--;
                    }
                    if (!put.isEmpty()) {
                        Collections.reverse(put);
                        this.distinguishTable[secondObject.index()][firstObject.index()].addAll(put);
                        this.distinguishTable[firstObject.index()][secondObject.index()].addAll(put);
                    }
                }
        }
        return distinguishTable;
    }

    public ArrayList<Set> getColumn(int numberOfColumn) {
        ArrayList<Set> expectedColumn = new ArrayList<>();
        for (int i = 0; i < this.numberOfObjects; i++) {
            expectedColumn.add(this.distinguishTable[i][numberOfColumn]);
        }
        return expectedColumn;
    }


    public Map<Integer, Integer> getListOfAttributeFrequency() {
        Map<Integer, Integer> frequencyOfAttributes = new HashMap<>();
        for (Integer attribute : this.listOfAttributes) {
            frequencyOfAttributes.put(attribute, Collections.frequency(listOfAttributes, attribute));
        }

        return MapUtil.sortByValue(frequencyOfAttributes);
    }

    /*
    Notice that this method is copied from StackOverflow - maybe we should rewrite this method?
     */
    private static class MapUtil {
        public static <K, V extends Comparable<? super V>> Map<K, V>
        sortByValue(Map<K, V> map) {
            List<Map.Entry<K, V>> list =
                    new LinkedList<>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
                public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                    return (o1.getValue()).compareTo(o2.getValue());
                }
            });

            Map<K, V> result = new LinkedHashMap<>();
            for (Map.Entry<K, V> entry : list) {
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }
    }

}
