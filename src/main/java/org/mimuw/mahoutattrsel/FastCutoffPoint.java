package org.mimuw.mahoutattrsel;

import org.apache.mahout.common.Pair;
import org.mimuw.mahoutattrsel.api.CutoffPointCalculator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * This implementations calculate number of significant attributes.
 * The model which is implemented  was published in a paper "Random Reducts: A Monte Carlo Rough Set-based Method
 * for Feature Selection in Large Datasets"
 */
public class FastCutoffPoint implements CutoffPointCalculator {

    @Override
    public List<Integer> calculateCutoffPoint(List<Double> scores) {

        int numberOfSignificantAttributes = 0;
        double valueOfModel = Double.POSITIVE_INFINITY;
        int numberOfAllAttributes = scores.size();
        List<Integer> listOfSignificantAttributes = new ArrayList<>();

        List<Pair<Integer, Double>> scoresWithNumbersOfAttributes = new ArrayList<>();
        buildScoresWithNumbersOfAttributes(scores, scoresWithNumbersOfAttributes);

        //decreasing sort
        Collections.sort(scoresWithNumbersOfAttributes, new Comparator<Pair<Integer, Double>>() {
            @Override
            public int compare(Pair<Integer, Double> o1, Pair<Integer, Double> o2) {
                return -o1.getSecond().compareTo(o2.getSecond());
            }
        });

        double scoreOfAllAttributes = sumScore(scoresWithNumbersOfAttributes, numberOfAllAttributes);

        for (int i = 1; i <= numberOfAllAttributes; i++) {
            double ithScore = sumScore(scoresWithNumbersOfAttributes, i);
            double fir = (1 - ithScore / scoreOfAllAttributes) * (1 - ithScore / scoreOfAllAttributes);
            double sec = ((double) i / (double) numberOfAllAttributes) * ((double) i / (double) numberOfAllAttributes);
            fir += sec;

            if (fir < valueOfModel) {
                valueOfModel = fir;
                numberOfSignificantAttributes = i;
            }
        }

        for (int i = 0; i < numberOfSignificantAttributes; i++) {
            listOfSignificantAttributes.add(scoresWithNumbersOfAttributes.get(i).getFirst());
        }

        return listOfSignificantAttributes;
    }

    private void buildScoresWithNumbersOfAttributes(List<Double> scores,
                                                    List<Pair<Integer, Double>> scoresWithNumbersOfAttributes) {
        for (int i = 0; i < scores.size(); i++) {
            scoresWithNumbersOfAttributes.add(new Pair<>(i, scores.get(i)));
        }
    }

    private double sumScore(List<Pair<Integer, Double>> sortScores, int numberOfAttributesToScore) {

        double result = 0;

        for (int i = 0; i < numberOfAttributesToScore; i++) {
            result += sortScores.get(i).getSecond();
        }
        return result;
    }
}
