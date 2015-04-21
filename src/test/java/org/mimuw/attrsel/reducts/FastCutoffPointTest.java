package org.mimuw.attrsel.reducts;

import com.google.common.collect.ImmutableList;
import org.mimuw.attrsel.common.FastCutoffPoint;
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
        scores.addAll(ImmutableList.of(2.0, 1.0, 3.0, 5.0));

        List<Integer> expectedListOfAttributes = new ArrayList<>();
        expectedListOfAttributes.addAll(ImmutableList.of(3, 2));

        FastCutoffPoint toTest = new FastCutoffPoint();
        assertThat(toTest.calculateCutoffPoint(scores)).isEqualTo(expectedListOfAttributes);
    }

    @Test
    public void testOneElementInScore() throws Exception {

        List<Double> scores = new ArrayList<>();
        scores.addAll(ImmutableList.of(6.777));

        List<Integer> expectedListOfAttributes = new ArrayList<>();
        expectedListOfAttributes.addAll(ImmutableList.of(0));

        FastCutoffPoint toTest = new FastCutoffPoint();
        assertThat(toTest.calculateCutoffPoint(scores)).isEqualTo(expectedListOfAttributes);
    }

    @Test
    public void testTwentyAttributes() throws Exception {


        List<Double> scores = new ArrayList<>();
        List<Double> scoresVector = new ArrayList<>();
        List<Integer> expectedListOfAttributes = new ArrayList<>();
        List<Integer> expectedListOfAttributesVector = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            scoresVector.add(i, (double) (i + 1));
        }

        scores.addAll(ImmutableList.copyOf(scoresVector));

        for (int i = 19; i > 11; i--) {
            expectedListOfAttributesVector.add(i);
        }

        expectedListOfAttributes.addAll(ImmutableList.copyOf(expectedListOfAttributesVector));

        FastCutoffPoint toTest = new FastCutoffPoint();
        assertThat(toTest.calculateCutoffPoint(scores)).isEqualTo(expectedListOfAttributes);
    }

    @Test
    public void testAllScoresEqualsOne() throws Exception {

        List<Double> scores = new ArrayList<>();
        scores.addAll(ImmutableList.of(1.0, 1.0, 1.0, 1.0));

        List<Integer> expectedListOfAttributes = new ArrayList<>();
        expectedListOfAttributes.addAll(ImmutableList.of(0, 1));

        FastCutoffPoint toTest = new FastCutoffPoint();
        assertThat(toTest.calculateCutoffPoint(scores)).isEqualTo(expectedListOfAttributes);
    }

    @Test
    public void testAllDoubleScores() throws Exception {

        List<Double> scores = new ArrayList<>();
        scores.addAll(ImmutableList.of(1.123, 2.333, 3.321));

        List<Integer> expectedListOfAttributes = new ArrayList<>();
        expectedListOfAttributes.addAll(ImmutableList.of(2));

        FastCutoffPoint toTest = new FastCutoffPoint();
        assertThat(toTest.calculateCutoffPoint(scores)).isEqualTo(expectedListOfAttributes);
    }

    @Test
    public void testEmptyScores() throws Exception {

        List<Double> scores = new ArrayList<>();
        List<Integer> expectedListOfAttributes = new ArrayList<>();
        FastCutoffPoint toTest = new FastCutoffPoint();
        assertThat(toTest.calculateCutoffPoint(scores)).isEqualTo(expectedListOfAttributes);
    }
}
