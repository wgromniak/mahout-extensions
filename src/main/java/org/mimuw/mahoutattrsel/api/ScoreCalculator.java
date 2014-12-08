package org.mimuw.mahoutattrsel.api;

/**
 * The implementations should be instantiated with the additional (apart from attribute id) information they need to
 * calculate the score.
 *
 * @param <T> type of attribute id; mostly {@link String} or {@link Integer}
 */
public interface ScoreCalculator<T> {

    double getScore(T attributeId);
}
