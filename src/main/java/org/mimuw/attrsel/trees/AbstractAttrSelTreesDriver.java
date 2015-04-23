package org.mimuw.attrsel.trees;

import org.mimuw.attrsel.common.AbstractAttrSelDriver;

public abstract class AbstractAttrSelTreesDriver extends AbstractAttrSelDriver {

    protected void setUpTreesOptions() {
        addOption("numberOfTrees", "numTrees", "Number of trees in each map task");
        addOption("u", "u", "u param from the paper");
        addOption("v", "v", "v param from the paper");
    }
}
