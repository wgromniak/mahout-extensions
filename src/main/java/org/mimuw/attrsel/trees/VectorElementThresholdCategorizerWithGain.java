package org.mimuw.attrsel.trees;

import gov.sandia.cognition.learning.function.categorization.VectorElementThresholdCategorizer;

public class VectorElementThresholdCategorizerWithGain extends VectorElementThresholdCategorizer {

    protected double gain;

    public VectorElementThresholdCategorizerWithGain(int index, double threshold, double gain) {
        super(index, threshold);
        this.gain = gain;
    }

    public double getGain() {
        return gain;
    }
}
