package org.mimuw.mahoutattrsel;

import org.testng.annotations.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * assertion for the FrequencyScoreCalculator.getScore() method
 */
public class FrequencyScoreCalculatorTest {
    @Test
    public void testScore() throws Exception {

        int TestCaseSize = 100, maxSubSize = 100, subsetAttrSize, reductSize;
        double result;
        Random generator = new Random();
        FrequencyScoreCalculator TestCalc;
        ArrayList<List<Integer>> reducts = new ArrayList<>();
        for (int i = 0; i < TestCaseSize; i++)
        {
            subsetAttrSize = generator.nextInt(maxSubSize)+1;
            reductSize = generator.nextInt(subsetAttrSize)+1;
            result = (double) reductSize/subsetAttrSize;
            for (int j = 0; j < reductSize; j++)
            {
                reducts.add(Arrays.asList(1,4,5));
            }
            TestCalc = new FrequencyScoreCalculator(reducts, subsetAttrSize);
            assertThat(TestCalc.getScore()).isEqualTo(result);
            reducts.clear();
        }
    }
}
