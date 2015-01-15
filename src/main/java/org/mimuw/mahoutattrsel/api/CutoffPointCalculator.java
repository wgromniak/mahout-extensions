package org.mimuw.mahoutattrsel.api;

import java.util.List;


public interface CutoffPointCalculator {

    List<Integer> calculateCutoffPoint(List<Double> scores);

}
