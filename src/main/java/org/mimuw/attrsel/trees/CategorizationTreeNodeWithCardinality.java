package org.mimuw.attrsel.trees;

import gov.sandia.cognition.learning.algorithm.tree.CategorizationTreeNode;
import gov.sandia.cognition.learning.algorithm.tree.DecisionTreeNode;

public class CategorizationTreeNodeWithCardinality<InputType, OutputType, InteriorType>
        extends CategorizationTreeNode<InputType, OutputType, InteriorType> {

    protected int cardinality;

    public CategorizationTreeNodeWithCardinality(
            DecisionTreeNode<InputType, OutputType> parent,
            OutputType outputCategory,
            int cardinality) {
        super(parent, outputCategory);
        this.cardinality = cardinality;
    }

    public int getCardinality() {
        return cardinality;
    }
}
