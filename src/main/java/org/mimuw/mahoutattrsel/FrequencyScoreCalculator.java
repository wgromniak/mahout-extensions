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
public class FrequencyScoreCalculator implements ScoreCalculator {

    private Iterable<List<Integer>> AttrReducts;
    private Integer AttrSubsetsCount;

    public FrequencyScoreCalculator(Iterable<List<Integer>> Reducts, Integer SubsetsCount)
    {
        checkNotNull(Reducts);
        checkArgument(SubsetsCount > 0);
        AttrReducts = Reducts;
        AttrSubsetsCount = SubsetsCount;
    }

    public double getScore()
    {
        return (double) Iterables.size(AttrReducts)/AttrSubsetsCount;
    }
}
