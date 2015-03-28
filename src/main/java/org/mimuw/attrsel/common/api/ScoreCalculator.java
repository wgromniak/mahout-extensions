package org.mimuw.attrsel.common.api;

/**
 * A single instance of ScoreCalculator calculates score for a fixed attribute. The implementations should be
 * instantiated with additional information they need to calculate the score.
 */
public interface ScoreCalculator {

    double getScore();
}
