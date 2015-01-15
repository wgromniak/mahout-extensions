package org.mimuw.mahoutattrsel;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Answers to all test was obtained by hand
 */
public class FastCutoffPointTest {

    @Test
    public void testCalculateCutoffPoint() throws Exception {

        List<Double> scores = new ArrayList<>();
        scores.add(0, 2.0);
        scores.add(1, 1.0);
        scores.add(2, 3.0);
        scores.add(3, 5.0);

        List<Integer> expectedListOfAttributes = new ArrayList<>();
        expectedListOfAttributes.add(3);
        expectedListOfAttributes.add(2);

        FastCutoffPoint toTest = new FastCutoffPoint();
        assertThat(toTest.calculateCutoffPoint(scores)).isEqualTo(expectedListOfAttributes);
    }

    @Test
    public void testOneElementInScore() throws Exception {

        List<Double> scores = new ArrayList<>();
        scores.add(0, 6.77);

        List<Integer> expectedListOfAttributes = new ArrayList<>();
        expectedListOfAttributes.add(0);

        FastCutoffPoint toTest = new FastCutoffPoint();
        assertThat(toTest.calculateCutoffPoint(scores)).isEqualTo(expectedListOfAttributes);
    }

    @Test
    public void testTwentyAttributes() throws Exception {


        List<Double> scores = new ArrayList<>();
        List<Integer> expectedListOfAttributes = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            scores.add(i, (double) (i + 1));
        }

        for (int i = 19; i > 11; i--) {
            expectedListOfAttributes.add(i);
        }

        FastCutoffPoint toTest = new FastCutoffPoint();
        assertThat(toTest.calculateCutoffPoint(scores)).isEqualTo(expectedListOfAttributes);
    }
}
