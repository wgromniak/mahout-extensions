package org.mimuw.mahoutattrsel;

import org.mimuw.mahoutattrsel.api.CutoffPointCalculator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This implementation calculate number of significant attributes and returns list whit position of these attributes
 * which is sorted according to the highest scores.
 * The model which is implemented  was published in a paper "Random Reducts: A Monte Carlo Rough Set-based Method
 * for Feature Selection in Large Datasets" 3.4.2 equation (5)
 */
public class FastCutoffPoint implements CutoffPointCalculator {

    @Override
    public List<Integer> calculateCutoffPoint(List<Double> scores) {

        int numberOfSignificantAttributes = 0;
        double valueOfModel = Double.POSITIVE_INFINITY;
        int numberOfAllAttributes = scores.size();

        List<Integer> listOfSignificantAttributes = new ArrayList<>();
        List<Double> scoresCopy = new ArrayList<>(scores);

        //Clone scores
        List<Double> sortScores = new ArrayList<>(scores);
        Collections.sort(sortScores);
        Collections.reverse(sortScores);

        double scoreOfAllAttributes = sumScore(sortScores, numberOfAllAttributes);

        for (int i = 1; i <= numberOfAllAttributes; i++) {
            double ithSumOfScoresScore = sumScore(sortScores, i);
            double fir = (1 - ithSumOfScoresScore / scoreOfAllAttributes) *
                    (1 - ithSumOfScoresScore / scoreOfAllAttributes);
            double sec = ((double) i / (double) numberOfAllAttributes) * ((double) i / (double) numberOfAllAttributes);
            double actualValueOfModel = fir + sec;

            if (actualValueOfModel < valueOfModel) {
                valueOfModel = actualValueOfModel;
                numberOfSignificantAttributes = i;
            }
        }

        for (int i = 0; i < numberOfSignificantAttributes; i++) {
            listOfSignificantAttributes.add(scoresCopy.indexOf(sortScores.get(i)));
            scoresCopy.add(scores.indexOf(sortScores.get(i)), null);
        }

        return listOfSignificantAttributes;
    }

    private double sumScore(List<Double> sortScores, int numberOfAttributesToScore) {

        double result = 0;

        for (int i = 0; i < numberOfAttributesToScore; i++) {
            result += sortScores.get(i);
        }
        return result;
    }
}
