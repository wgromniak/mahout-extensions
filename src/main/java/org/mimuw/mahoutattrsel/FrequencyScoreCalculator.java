package org.mimuw.mahoutattrsel;

import org.mimuw.mahoutattrsel.api.ScoreCalculator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import  com.google.common.collect.Iterables;

import java.util.List;

/**
 * This class counts FrequencyScore for fixed attribute. Class gets description of reducts and number of subsets
 * in which attribute occurs.
 */
public final class FrequencyScoreCalculator implements ScoreCalculator {

    private Iterable<List<Integer>> attrReducts;
    private int attrSubsetsCount;

    public FrequencyScoreCalculator(Iterable<List<Integer>> reducts, int subsetsCount) {
        checkNotNull(reducts, "Reducts must not be null");
        checkArgument(subsetsCount > 0, "SubsetCount must be positive");

        attrReducts = reducts;
        attrSubsetsCount = subsetsCount;
    }



    @Override
    public double getScore() {
        return (double) Iterables.size(attrReducts) / attrSubsetsCount;
    }
}

