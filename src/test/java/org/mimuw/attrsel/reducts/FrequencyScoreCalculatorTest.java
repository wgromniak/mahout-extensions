package org.mimuw.attrsel.reducts;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mimuw.attrsel.assertions.AttrselAssertions.assertThat;

/**
 * assertion for the FrequencyScoreCalculator.getScore() method
 */
public class FrequencyScoreCalculatorTest {

    @Test
    public void testScore() throws Exception {

        double result = 0.3;
        ArrayList<List<Integer>> reducts = new ArrayList<>();
        FrequencyScoreCalculator testCalc;

        fillReducts(3, reducts);
        testCalc = new FrequencyScoreCalculator(reducts, 10);
        assertThat(testCalc.getScore()).isEqualTo(result);

        result = 0;
        fillReducts(0, reducts);
        testCalc = new FrequencyScoreCalculator(reducts, 10);
        assertThat(testCalc.getScore()).isEqualTo(result);

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testIllegalArgumentException() {
        ArrayList<List<Integer>> reducts = new ArrayList<>();
        reducts.add(Arrays.asList(1));
        new FrequencyScoreCalculator(reducts, 0);
    }

    private void fillReducts(int reductSize, ArrayList<List<Integer>> reducts) {

        reducts.clear();
        for (int j = 0; j < reductSize; j++) {
            reducts.add(Arrays.asList(1, 4, 5));
        }
    }

}
