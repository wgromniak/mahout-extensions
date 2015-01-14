package org.mimuw.mahoutattrsel;

import org.apache.mahout.math.Matrix;
import org.testng.annotations.Test;

import java.util.BitSet;
import java.util.Random;

import static org.mimuw.mahoutattrsel.assertions.AttrselAssertions.assertThat;
import static org.mockito.Mockito.mock;

public class AbstractMatrixFixedSizeSubtableGeneratorTest {

    @Test
    public void testAllSampleSeed() throws Exception {

        Random random = new Random(100);

        BitSet bitSetUnderTests;

        AbstractMatrixFixedSizeSubtableGenerator generatorUnderTest = new DummyGenerator(random);

        bitSetUnderTests = generatorUnderTest.draw(10, 10);

        BitSet expected = new BitSet();

        for (int i = 0; i < 10; i++) {
            expected.set(i);
        }

        assertThat(bitSetUnderTests).isEqualTo(expected);

    }

    @Test
    public void testThreeSampleFromTen() throws Exception {

        Random random = new Random(150);

        BitSet bitSetUnderTests;

        AbstractMatrixFixedSizeSubtableGenerator generatorUnderTest = new DummyGenerator(random);

        bitSetUnderTests = generatorUnderTest.draw(10, 3);

        BitSet expected = new BitSet();

        expected.set(1);
        expected.set(9);
        expected.set(7);

        assertThat(bitSetUnderTests).isEqualTo(expected);

    }

    @Test
    public void testTwoSampleFromBigSet() throws Exception {

        Random random = new Random(150);

        BitSet bitSetUnderTests;

        AbstractMatrixFixedSizeSubtableGenerator generatorUnderTest = new DummyGenerator(random);

        bitSetUnderTests = generatorUnderTest.draw(432, 2);

        BitSet expected = new BitSet();

        expected.set(323);
        expected.set(159);

        assertThat(bitSetUnderTests).isEqualTo(expected);

    }

    @Test
    public void testSevenFromBigSet() throws Exception {

        Random random = new Random(150);

        BitSet bitSetUnderTests;

        AbstractMatrixFixedSizeSubtableGenerator generatorUnderTest = new DummyGenerator(random);

        bitSetUnderTests = generatorUnderTest.draw(432, 7);

        BitSet expected = new BitSet();

        expected.set(323);
        expected.set(159);
        expected.set(201);
        expected.set(349);
        expected.set(111);
        expected.set(74);
        expected.set(362);

        assertThat(bitSetUnderTests).isEqualTo(expected);

    }

    @Test
    public void testThreeSamples() throws Exception {

        Random random = new Random(100);

        BitSet bitSetUnderTests;

        AbstractMatrixFixedSizeSubtableGenerator generatorUnderTest = new DummyGenerator(random);

        bitSetUnderTests = generatorUnderTest.draw(10, 1);

        BitSet expected = new BitSet();

        expected.set(5);

        assertThat(bitSetUnderTests).isEqualTo(expected);

    }

    private static final class DummyGenerator extends AbstractMatrixFixedSizeSubtableGenerator {

        private DummyGenerator(Random random) {
            super(random, 1, 1, mock(Matrix.class));
        }

        @Override
        void calculateSubtables() {
            throw new UnsupportedOperationException();
        }
    }
}