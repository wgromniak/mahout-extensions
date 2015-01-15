package org.mimuw.mahoutattrsel;

import org.mimuw.mahoutattrsel.api.CutoffPointCalculator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This implementations calculate number of significant attributes and returns list whit position these attributes
 * which is sorted according to the highest scores.
 * The model which is implemented  was published in a paper "Random Reducts: A Monte Carlo Rough Set-based Method
 * for Feature Selection in Large Datasets" 3.4.2 equation (5)
 */
public class FastCutoffPoint implements CutoffPointCalculator {

    @Override
    public List<Integer> calculateCutoffPoint(List<Double> scores) {

        int numberOfSignificantAttributes = Integer.MAX_VALUE;
        double valueOfModel = Double.POSITIVE_INFINITY;
        int numberOfAllAttributes = scores.size();
        List<Integer> listOfSignificantAttributes = new ArrayList<>();

        //Clone scores
        List<Double> sortScores = new ArrayList<>(scores);
        Collections.sort(sortScores);
        Collections.reverse(sortScores);

        int scoreOfAllAttributes = kScore(sortScores, numberOfAllAttributes);

        for (int i = 1; i <= numberOfAllAttributes; i++) {
            double numberOfIScore = kScore(sortScores, i);
            double fir = (1 - (numberOfIScore) / scoreOfAllAttributes) * (1 - (numberOfIScore) / scoreOfAllAttributes);
            double sec = ((double) i / (double) numberOfAllAttributes) * ((double) i / (double) numberOfAllAttributes);
            double actualValueOfModel = fir + sec;

            if (actualValueOfModel < valueOfModel) {
                valueOfModel = actualValueOfModel;
                numberOfSignificantAttributes = i;
            }
        }
        for (int i = 0; i < numberOfSignificantAttributes; i++) {
            listOfSignificantAttributes.add(scores.indexOf(sortScores.get(i)));
        }

        return listOfSignificantAttributes;
    }

    private int kScore(List<Double> sortScores, int numberOfAttributesToScore) {

        int result = 0;

        for (int i = 0; i < numberOfAttributesToScore; i++) {
            result += sortScores.get(i);
        }
        return result;
    }
}
