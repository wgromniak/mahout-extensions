package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.DenseMatrix;
import org.mimuw.mahoutattrsel.api.Subtable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MatrixFixedSizeObjectSubtableGeneratorTest {

    @Test
    public void testSimple() throws Exception {

        MatrixFixedSizeObjectSubtableGenerator subtableGenerator = new MatrixFixedSizeObjectSubtableGenerator(new Random(), 2, 3,
                new DenseMatrix(new double[][]{{1, 2, 3}, {7, 6, 5}, {3, 3, 3}}));

        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

        MatrixAssert.assertThat(listOfSubtables.get(0).getTable()).isEqualTo
                ((new DenseMatrix(new double[][]{{1, 2, 3}, {7, 6, 5}, {3, 3, 3}})));
        MatrixAssert.assertThat(listOfSubtables.get(1).getTable()).isEqualTo
                ((new DenseMatrix(new double[][]{{1, 2, 3}, {7, 6, 5}, {3, 3, 3}})));
        assertThat(listOfSubtableCounts).containsExactly(2, 2);
    }

    @Test
    public void testGetSubtablesWithStubbedRandom() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(0, 1);

        MatrixFixedSizeObjectSubtableGenerator subtableGenerator = new MatrixFixedSizeObjectSubtableGenerator(random, 1, 2,
                new DenseMatrix(new double[][]{{1, 2, 3}, {7, 6, 5}}));

        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

       MatrixAssert.assertThat(listOfSubtables.get(0).getTable()).isEqualTo(new DenseMatrix
               (new double[][]{{1, 2, 3}, {7, 6, 5}}));
        assertThat(listOfSubtableCounts).containsExactly(1, 1);
    }

    @Test
    public void testOneSubtableWithThreeObjectsPredictableRandom() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(3, 4);
        MatrixFixedSizeObjectSubtableGenerator subtableGenerator = new MatrixFixedSizeObjectSubtableGenerator(
                random, 1, 3, new DenseMatrix(new double[][]{{1, 2}, {3, 4}, {5, 6}, {7, 6}, {8, 9}}));

        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

        MatrixAssert.assertThat(listOfSubtables.get(0).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2}, {3, 4}, {5, 6}}));
        assertThat(listOfSubtableCounts).containsExactly(1);
    }

    @Test
    public void testThreeSubtableWithFourObjectsPredictableRandom() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(4, 3, 3, 3, 0, 1, 2, 4, 4, 2, 1, 0);
        MatrixFixedSizeObjectSubtableGenerator subtableGenerator = new MatrixFixedSizeObjectSubtableGenerator(
                random, 3, 4, new DenseMatrix(new double[][]{{1, 2}, {3, 4}, {5, 6}, {7, 6}, {8, 9}}));

        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

        MatrixAssert.assertThat(listOfSubtables.get(0).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2}, {3, 4}, {5, 6}, {7, 6}}));

        MatrixAssert.assertThat(listOfSubtables.get(1).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2}, {3, 4}, {5, 6}, {8, 9}}));

        MatrixAssert.assertThat(listOfSubtables.get(2).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2}, {3, 4}, {5, 6}, {8, 9}}));

        assertThat(listOfSubtableCounts).containsExactly(3);
    }

    @Test
    public void testOneDimensionalGetSubtable() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(0, 1, 2);

        MatrixFixedSizeObjectSubtableGenerator subtableGenerator = new MatrixFixedSizeObjectSubtableGenerator(random,
                2, 1, new DenseMatrix(new double[][]{{1, 2, 3}, {4, 5, 6}}));

        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

        MatrixAssert.assertThat(listOfSubtables.get(0).getTable()).isEqualTo(new DenseMatrix(new double[][]{{1, 2, 3}}));
        MatrixAssert.assertThat(listOfSubtables.get(1).getTable()).isEqualTo(new DenseMatrix(new double[][]{{4, 5, 6}}));
        assertThat(listOfSubtableCounts).containsExactly(2, 2);
    }

    @Test
    public void testThreeSubtablesTwoObjectSeed() throws Exception {

        Random random = new Random(177);

        MatrixFixedSizeObjectSubtableGenerator subtableGenerator = new MatrixFixedSizeObjectSubtableGenerator(
                random, 3, 2, new DenseMatrix(new double[][]{{4, 3, 3, 2}, {1, 2, 3, 4}, {5, 4, 3, 2},
                {4, 3, 2, 1}, {4, 2, 2, 1}, {5, 5, 5, 5}}));

        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

        MatrixAssert.assertThat(listOfSubtables.get(0).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{4, 3, 3, 2}, {1, 2, 3, 4}}));

        MatrixAssert.assertThat(listOfSubtables.get(1).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2, 3, 4}, {5, 5, 5, 5}}));

        MatrixAssert.assertThat(listOfSubtables.get(2).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{4, 3, 3, 2}, {1, 2, 3, 4}}));

        assertThat(listOfSubtableCounts).containsExactly(3, 3, 3);
    }

    @Test
    public void testOneObjectFiveSubtablesSeed() throws Exception {

        Random random = new Random(199);

        MatrixFixedSizeObjectSubtableGenerator subtableGenerator = new MatrixFixedSizeObjectSubtableGenerator(
                random, 5, 1, new DenseMatrix(new double[][]{{4, 3, 3, 2}, {1, 2, 3, 4}, {5, 4, 3, 2},
                {4, 3, 2, 1}, {4, 2, 2, 1}, {5, 5, 5, 5}}));

        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

        MatrixAssert.assertThat(listOfSubtables.get(0).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{5, 5, 5, 5}}));

        MatrixAssert.assertThat(listOfSubtables.get(1).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{5, 5, 5, 5}}));

        MatrixAssert.assertThat(listOfSubtables.get(3).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{4, 3, 3, 2}}));

        MatrixAssert.assertThat(listOfSubtables.get(4).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{5, 4, 3, 2}}));

        MatrixAssert.assertThat(listOfSubtables.get(2).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{4, 3, 2, 1}}));

        assertThat(listOfSubtableCounts).containsExactly(5, 5, 5);
    }
}
