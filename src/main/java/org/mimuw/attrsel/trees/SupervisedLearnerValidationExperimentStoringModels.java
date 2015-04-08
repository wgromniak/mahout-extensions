package org.mimuw.attrsel.trees;

import gov.sandia.cognition.evaluator.Evaluator;
import gov.sandia.cognition.learning.data.InputOutputPair;
import gov.sandia.cognition.learning.data.PartitionedDataset;
import gov.sandia.cognition.learning.experiment.SupervisedLearnerValidationExperiment;
import gov.sandia.cognition.learning.experiment.ValidationFoldCreator;
import gov.sandia.cognition.learning.performance.PerformanceEvaluator;
import gov.sandia.cognition.util.Summarizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class SupervisedLearnerValidationExperimentStoringModels<InputType, OutputType, StatisticType, SummaryType>
        extends SupervisedLearnerValidationExperiment<InputType, OutputType, StatisticType, SummaryType> {

    protected List<Evaluator<? super InputType, OutputType>> learned = new ArrayList<>();

    public SupervisedLearnerValidationExperimentStoringModels(List<Evaluator<? super InputType, OutputType>> learned) {
        this.learned = learned;
    }

    public SupervisedLearnerValidationExperimentStoringModels(
            ValidationFoldCreator<InputOutputPair<InputType, OutputType>, InputOutputPair<InputType, OutputType>> foldCreator,
            PerformanceEvaluator<? super Evaluator<? super InputType, ? extends OutputType>, Collection<? extends InputOutputPair<InputType, OutputType>>, ? extends StatisticType> performanceEvaluator,
            Summarizer<? super StatisticType, ? extends SummaryType> summarizer) {
        super(foldCreator, performanceEvaluator, summarizer);
    }

    @Override
    protected void runTrial(PartitionedDataset<InputOutputPair<InputType, OutputType>> fold) {
        // Perform the learning algorithm on this fold.
        final Evaluator<? super InputType, OutputType> learned = getLearner().learn(fold.getTrainingSet());

        this.learned.add(learned);

        // Compute the statistic of the learned object on the testing set.
        final Collection<InputOutputPair<InputType, OutputType>> testingSet = fold.getTestingSet();
        final StatisticType statistic =
                this.getPerformanceEvaluator().evaluatePerformance(
                        learned, testingSet);
        statistics.add(statistic);
    }

    public List<Evaluator<? super InputType, OutputType>> getLearned() {
        return learned;
    }
}
