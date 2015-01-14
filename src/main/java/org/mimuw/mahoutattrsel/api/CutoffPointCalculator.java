package org.mimuw.mahoutattrsel.api;

import java.util.List;


public interface CutoffPointCalculator {

    int calculateCutoffPoint(List<Double> scores);

}
