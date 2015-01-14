package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.DenseMatrix;
import org.mimuw.mahoutattrsel.api.Subtable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;

import static org.mimuw.mahoutattrsel.assertions.AttrselAssertions.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MatrixFixedSizeAttributeSubtableGeneratorTest {

    @Test
    public void testThreeSubtableOneAttribute() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(0, 1, 2, 3);

        MatrixFixedSizeAttributeSubtableGenerator subtableGenerator = new MatrixFixedSizeAttributeSubtableGenerator(
                random, 3, 1, new DenseMatrix(new double[][]{{1, 2, 2, 6}, {3, 4, 6, 4}, {5, 6, 8, 9}, {7, 6, 3, 6}}));

        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

        assertThat(listOfSubtables.get(0).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{1, 6}, {3, 4}, {5, 9}, {7, 6}}));

        assertThat(listOfSubtables.get(0).getNumberOfAttributes()).isEqualTo(1);
        assertThat(listOfSubtables.get(0).getOriginalNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(0).iterateAttributes()).containsExactly(0);

        assertThat(listOfSubtables.get(1).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{2, 6}, {4, 4}, {6, 9}, {6, 6}}));

        assertThat(listOfSubtables.get(1).getNumberOfAttributes()).isEqualTo(1);
        assertThat(listOfSubtables.get(1).getOriginalNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(1).iterateAttributes()).containsExactly(1);


        assertThat(listOfSubtables.get(2).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{2, 6}, {6, 4}, {8, 9}, {3, 6}}));

        assertThat(listOfSubtables.get(2).getNumberOfAttributes()).isEqualTo(1);
        assertThat(listOfSubtables.get(2).getOriginalNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(2).iterateAttributes()).containsExactly(2);
        assertThat(listOfSubtableCounts).containsExactly(1, 1, 1);
    }

    @Test
    public void testOneSubtablesThreeAttributes() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(0, 1, 2, 3, 4, 5, 6, 7);

        MatrixFixedSizeAttributeSubtableGenerator subtableGenerator = new MatrixFixedSizeAttributeSubtableGenerator(
                new Random(), 1, 3, new DenseMatrix(new double[][]{{1, 2, 3, 4}, {5, 6, 7, 8}}));


        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

        assertThat(listOfSubtables.get(0).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2, 3, 4}, {5, 6, 7, 8}}));

        assertThat(listOfSubtables.get(0).getNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(0).getOriginalNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(0).iterateAttributes()).containsExactly(0, 1, 2);
        assertThat(listOfSubtableCounts).containsExactly(1, 1, 1);
    }

    @Test
    public void testThreeSubtablesTwoAttributes() throws Exception {

        Random random = mock(Random.class);

        when(random.nextInt(anyInt())).thenReturn(1, 0, 2, 2, 1, 1);

        MatrixFixedSizeAttributeSubtableGenerator subtableGenerator = new MatrixFixedSizeAttributeSubtableGenerator(
                random, 3, 2, new DenseMatrix(new double[][]{{1, 2, 2, 6}, {3, 4, 6, 4}, {5, 6, 8, 9}, {7, 6, 3, 6}}));

        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

        assertThat(listOfSubtables.get(0).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2, 6}, {3, 6, 4}, {5, 8, 9}, {7, 3, 6}}));

        assertThat(listOfSubtables.get(0).getNumberOfAttributes()).isEqualTo(2);
        assertThat(listOfSubtables.get(0).getOriginalNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(0).iterateAttributes()).containsExactly(0, 2);

        assertThat(listOfSubtables.get(1).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{2, 2, 6}, {4, 6, 4}, {6, 8, 9}, {6, 3, 6}}));

        assertThat(listOfSubtables.get(1).getNumberOfAttributes()).isEqualTo(2);
        assertThat(listOfSubtables.get(1).getOriginalNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(1).iterateAttributes()).containsExactly(1, 2);

        assertThat(listOfSubtables.get(2).getTable()).isEqualTo(
                new DenseMatrix(new double[][]{{1, 2, 6}, {3, 4, 4}, {5, 6, 9}, {7, 6, 6}}));

        assertThat(listOfSubtables.get(2).getNumberOfAttributes()).isEqualTo(2);
        assertThat(listOfSubtables.get(2).getOriginalNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(2).iterateAttributes()).containsExactly(0, 1);

        assertThat(listOfSubtableCounts).containsExactly(2, 2, 2);
    }

    @Test
    public void testThreeSubtablesTwoAttributesSeed() throws Exception {

        Random random = new Random(100);

        MatrixFixedSizeAttributeSubtableGenerator subtableGenerator = new MatrixFixedSizeAttributeSubtableGenerator(
                random, 3, 2, new DenseMatrix(new double[][]{{1, 2, 3, 4, 5}, {4, 4, 5, 1, 3}, {5, 4, 2, 1, 3},
                {4, 2, 1, 3, 4}, {5, 6, 7, 4, 1}, {4, 44, 21, 2, 3}}));

        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

        assertThat(listOfSubtables.get(0).getTable()).isEqualTo(new DenseMatrix(
                new double[][]{{1, 3, 5}, {4, 5, 3}, {5, 2, 3}, {4, 1, 4}, {5, 7, 1}, {4, 21, 3}}));

        assertThat(listOfSubtables.get(0).getNumberOfAttributes()).isEqualTo(2);
        assertThat(listOfSubtables.get(0).getOriginalNumberOfAttributes()).isEqualTo(4);
        assertThat(listOfSubtables.get(0).iterateAttributes()).containsExactly(0, 2);

        assertThat(listOfSubtables.get(1).getTable()).isEqualTo(new DenseMatrix(
                new double[][]{{1, 3, 5}, {4, 5, 3}, {5, 2, 3}, {4, 1, 4}, {5, 7, 1}, {4, 21, 3}}));

        assertThat(listOfSubtables.get(1).getNumberOfAttributes()).isEqualTo(2);
        assertThat(listOfSubtables.get(1).getOriginalNumberOfAttributes()).isEqualTo(4);
        assertThat(listOfSubtables.get(1).iterateAttributes()).containsExactly(0, 2);

        assertThat(listOfSubtables.get(2).getTable()).isEqualTo(new DenseMatrix(
                new double[][]{{2, 4, 5}, {4, 1, 3}, {4, 1, 3}, {2, 3, 4}, {6, 4, 1}, {44, 2, 3}}));

        assertThat(listOfSubtables.get(2).getNumberOfAttributes()).isEqualTo(2);
        assertThat(listOfSubtables.get(2).getOriginalNumberOfAttributes()).isEqualTo(4);
        assertThat(listOfSubtables.get(2).iterateAttributes()).containsExactly(1, 3);

        assertThat(listOfSubtableCounts).containsExactly(2, 1, 2, 1);
    }

    @Test
    public void testTwoSubtablesThreeAttributesSeed() throws Exception {

        Random random = new Random(36);

        MatrixFixedSizeAttributeSubtableGenerator subtableGenerator = new MatrixFixedSizeAttributeSubtableGenerator(
                random, 2, 3, new DenseMatrix(new double[][]{{1, 2, 3, 4, 5}, {4, 4, 5, 1, 3}, {5, 4, 2, 1, 3},
                {4, 2, 1, 3, 4}, {5, 6, 7, 4, 1}, {4, 44, 21, 2, 3}}));

        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

        assertThat(listOfSubtables.get(0).getTable()).isEqualTo(new DenseMatrix(new double[][]
                {{1, 2, 4, 5}, {4, 4, 1, 3}, {5, 4, 1, 3},
                        {4, 2, 3, 4}, {5, 6, 4, 1}, {4, 44, 2, 3}}));

        assertThat(listOfSubtables.get(0).getNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(0).getOriginalNumberOfAttributes()).isEqualTo(4);
        assertThat(listOfSubtables.get(0).iterateAttributes()).containsExactly(0, 1, 3);

        assertThat(listOfSubtables.get(0).getTable()).isEqualTo(new DenseMatrix(new double[][]
                {{1, 2, 4, 5}, {4, 4, 1, 3}, {5, 4, 1, 3},
                        {4, 2, 3, 4}, {5, 6, 4, 1}, {4, 44, 2, 3}}));

        assertThat(listOfSubtables.get(0).getNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(0).getOriginalNumberOfAttributes()).isEqualTo(4);
        assertThat(listOfSubtables.get(0).iterateAttributes()).containsExactly(0, 1, 3);

        assertThat(listOfSubtableCounts).containsExactly(2, 2, 0, 2);
    }

    @Test
    public void testFiveSubtablesOneAttribute() throws Exception {

        Random random = new Random(175);

        MatrixFixedSizeAttributeSubtableGenerator subtableGenerator = new MatrixFixedSizeAttributeSubtableGenerator(
                random, 5, 1, new DenseMatrix(new double[][]{{4, 3, 3, 2}, {1, 2, 3, 4}, {5, 4, 3, 2},
                {4, 3, 2, 1}, {4, 2, 2, 1}, {5, 5, 5, 5}}));

        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

        assertThat(listOfSubtables.get(0).getTable()).isEqualTo(new DenseMatrix(new double[][]
                {{3, 2}, {2, 4}, {4, 2}, {3, 1}, {2, 1}, {5, 5}}));

        assertThat(listOfSubtables.get(0).getNumberOfAttributes()).isEqualTo(1);
        assertThat(listOfSubtables.get(0).getOriginalNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(0).iterateAttributes()).containsExactly(1);

        assertThat(listOfSubtables.get(1).getTable()).isEqualTo(new DenseMatrix(new double[][]
                {{3, 2}, {3, 4}, {3, 2}, {2, 1}, {2, 1}, {5, 5}}));

        assertThat(listOfSubtables.get(1).getNumberOfAttributes()).isEqualTo(1);
        assertThat(listOfSubtables.get(1).getOriginalNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(1).iterateAttributes()).containsExactly(2);

        assertThat(listOfSubtables.get(2).getTable()).isEqualTo(new DenseMatrix(new double[][]
                {{4, 2}, {1, 4}, {5, 2}, {4, 1}, {4, 1}, {5, 5}}));

        assertThat(listOfSubtables.get(2).getNumberOfAttributes()).isEqualTo(1);
        assertThat(listOfSubtables.get(2).getOriginalNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(2).iterateAttributes()).containsExactly(0);

        assertThat(listOfSubtables.get(3).getTable()).isEqualTo(new DenseMatrix(new double[][]
                {{3, 2}, {2, 4}, {4, 2}, {3, 1}, {2, 1}, {5, 5}}));

        assertThat(listOfSubtables.get(3).getNumberOfAttributes()).isEqualTo(1);
        assertThat(listOfSubtables.get(3).getOriginalNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(3).iterateAttributes()).containsExactly(1);

        assertThat(listOfSubtables.get(4).getTable()).isEqualTo(new DenseMatrix(new double[][]
                {{3, 2}, {2, 4}, {4, 2}, {3, 1}, {2, 1}, {5, 5}}));

        assertThat(listOfSubtables.get(4).getNumberOfAttributes()).isEqualTo(1);
        assertThat(listOfSubtables.get(4).getOriginalNumberOfAttributes()).isEqualTo(3);
        assertThat(listOfSubtables.get(4).iterateAttributes()).containsExactly(1);

        assertThat(listOfSubtableCounts).containsExactly(1, 3, 1);
    }

    @Test
    public void testFourAttributesOneSubtable() throws Exception {

        Random random = new Random(2024434);

        MatrixFixedSizeAttributeSubtableGenerator subtableGenerator = new MatrixFixedSizeAttributeSubtableGenerator(
                random, 1, 4, new DenseMatrix(new double[][]{{4, 5, 6, 3, 3, 2}, {1, 5, 6, 2, 3, 4}, {5, 4, 4, 4, 3, 2},
                {4, 3, 5, 3, 2, 1}, {4, 2, 6, 3, 2, 1}, {5, 5, 5, 5, 5, 5}}));

        List<Subtable> listOfSubtables = subtableGenerator.getSubtables();
        List<Integer> listOfSubtableCounts = subtableGenerator.getNumberOfSubtablesPerAttribute();

        assertThat(listOfSubtables.get(0).getTable()).isEqualTo(new DenseMatrix(new double[][]
                {{5, 6, 3, 3, 2}, {5, 6, 2, 3, 4}, {4, 4, 4, 3, 2}, {3, 5, 3, 2, 1},
                        {2, 6, 3, 2, 1}, {5, 5, 5, 5, 5}}));

        assertThat(listOfSubtables.get(0).getNumberOfAttributes()).isEqualTo(4);
        assertThat(listOfSubtables.get(0).getOriginalNumberOfAttributes()).isEqualTo(5);
        assertThat(listOfSubtables.get(0).iterateAttributes()).containsExactly(1, 2, 3, 4);

        assertThat(listOfSubtableCounts).containsExactly(0, 1, 1, 1, 1);
    }
}




