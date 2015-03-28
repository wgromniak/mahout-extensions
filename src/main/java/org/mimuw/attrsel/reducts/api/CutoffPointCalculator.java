package org.mimuw.attrsel.reducts.api;

import java.util.List;


public interface CutoffPointCalculator {

    List<Integer> calculateCutoffPoint(List<Double> scores);

}
