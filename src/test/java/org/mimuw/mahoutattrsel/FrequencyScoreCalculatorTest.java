package org.mimuw.mahoutattrsel;

import org.testng.annotations.Test;

import java.util.*;


import static org.assertj.core.api.Assertions.assertThat;

/**
 * assertion for the FrequencyScoreCalculator.getScore() method
 */
public class FrequencyScoreCalculatorTest {



    private void fillReducts(int reductSize, ArrayList<List<Integer>> reducts) {

        reducts.clear();
        for (int j = 0; j < reductSize; j++) {
            reducts.add(Arrays.asList(1, 4, 5));
        }
    }

    @Test
    public void testScore() throws Exception {

        double result = (double) 3/10;
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

    @Test
    public void testIllegalArgumentException() {
        try {
            ArrayList<List<Integer>> reducts = new ArrayList<>();
            reducts.add(Arrays.asList(1));
            FrequencyScoreCalculator testCalc;
            testCalc = new FrequencyScoreCalculator(reducts, 0);
            testCalc.getScore();
            assert false;
        } catch (IllegalArgumentException e) {
            assert true;
        }
    }

}
