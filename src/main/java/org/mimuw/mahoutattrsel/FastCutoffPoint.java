package org.mimuw.mahoutattrsel;

import org.mimuw.mahoutattrsel.api.CutoffPointCalculator;

import java.util.Collections;
import java.util.List;


public class FastCutoffPoint implements CutoffPointCalculator {

    @Override
    public int calculateCutoffPoint(List<Double> scores) {

        int numberOfSignificantAttributes = Integer.MAX_VALUE;
        double valueOfModel = Double.POSITIVE_INFINITY;
        int numberOfAllAttributes = scores.size();

        Collections.sort(scores);
        Collections.reverse(scores);

        int scoreOfAllAttributes = kScore(scores, numberOfAllAttributes);

        for (int i = 1; i <= numberOfAllAttributes; i++) {
            double numberOfIScore = kScore(scores, i);
            double fir = (1 - (numberOfIScore) / scoreOfAllAttributes) * (1 - (numberOfIScore) / scoreOfAllAttributes);
            double sec = ((double) i / (double) numberOfAllAttributes) * ((double) i / (double) numberOfAllAttributes);
            fir += sec;

            if (fir < valueOfModel) {
                valueOfModel = fir;
                numberOfSignificantAttributes = i;
            }
        }
        return numberOfSignificantAttributes;
    }


    private  int kScore(List<Double> scores, int ile ) {

        int result = 0;

        for (int i = 0; i < ile; i++) {
            result += scores.get(i);
        }
        return result;
    }
}
