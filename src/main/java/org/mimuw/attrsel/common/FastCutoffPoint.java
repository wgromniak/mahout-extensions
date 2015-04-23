package org.mimuw.attrsel.common;

import org.apache.mahout.common.Pair;
import org.mimuw.attrsel.common.api.CutoffPointCalculator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This implementations calculate number of significant attributes.
 * The model which is implemented  was published in a paper "Random Reducts: A Monte Carlo Rough Set-based Method
 * for Feature Selection in Large Datasets"
 */
public class FastCutoffPoint implements CutoffPointCalculator {

    private final int numberOfIterations;

    public FastCutoffPoint(int numberOfIterations) {
        checkArgument(numberOfIterations > 0, "Expected positive numberOfIterations");
        this.numberOfIterations = numberOfIterations;
    }

    public FastCutoffPoint() {
        this.numberOfIterations = 1;
    }

    @Override
    public List<Integer> calculateCutoffPoint(List<Double> scores) {

        List<Pair<Integer, Double>> scoresWithNumbersOfAttributes = new ArrayList<>();
        buildScoresWithNumbersOfAttributes(scores, scoresWithNumbersOfAttributes);

        //decreasing sort
        Collections.sort(scoresWithNumbersOfAttributes, new Comparator<Pair<Integer, Double>>() {
            @Override
            public int compare(Pair<Integer, Double> o1, Pair<Integer, Double> o2) {
                return -o1.getSecond().compareTo(o2.getSecond());
            }
        });

        List<Pair<Integer, Double>> filtered = cutOff(scoresWithNumbersOfAttributes);

        for (int i = 0; i < (numberOfIterations - 1); i++) {
            filtered = cutOff(filtered);
        }

        List<Integer> listOfSignificantAttributes = new ArrayList<>(filtered.size());

        for (Pair<Integer, Double> attrScore : filtered) {
            listOfSignificantAttributes.add(attrScore.getFirst());
        }

        return listOfSignificantAttributes;
    }

    private List<Pair<Integer, Double>> cutOff(List<Pair<Integer, Double>> scoresWithNumbersOfAttributes) {
        int numberOfSignificantAttributes = 0;
        double valueOfModel = Double.POSITIVE_INFINITY;
        int numberOfAllAttributes = scoresWithNumbersOfAttributes.size();

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

        List<Pair<Integer, Double>> filtered = new ArrayList<>(numberOfSignificantAttributes);

        for (int i = 0; i < numberOfSignificantAttributes; i++) {
            filtered.add(scoresWithNumbersOfAttributes.get(i));
        }

        return filtered;
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
