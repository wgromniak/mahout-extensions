package org.mimuw.attrsel.reducts;

import org.mimuw.attrsel.common.AbstractAttrSelDriver;
import org.mimuw.attrsel.common.api.Subtable;
import rseslib.processing.discretization.ChiMergeDiscretizationProvider;
import rseslib.processing.reducts.ReductsProvider;
import rseslib.system.PropertyConfigurationException;

public abstract class AbstractAttrSelReductsDriver extends AbstractAttrSelDriver {

    protected void setUpReductsOptions() {
        addOption("ReductsProvider", "redprov", "Reducts provider class", "org.mimuw.attrsel.reducts.JohnsonReductsProvider");
        addOption("IndiscernibilityForMissing", "indisc", "Indiscernibility for missing values");
        addOption("DiscernibilityMethod", "discMeth", "Discernibility method");
        addOption("GeneralizedDecisionTransitiveClosure", "genDec", "Generalized decision transitive closure");
        addOption("JohnsonReducts", "johnson", "Johnson reducts");
        addFlag("noDiscretize", "noDisc", "Whether the data should NOT be discretized");
        addOption("numDiscIntervals", "numDicsInt", "Number of discretization intervals");
        addOption("discSignificance", "discSig", "Chi merge discretization significance");
    }

    @SuppressWarnings("unchecked")
    protected Class<? extends ReductsProvider> getReductsProviderClass() {
        try {
            return (Class<? extends ReductsProvider>) Class.forName(getOption("ReductsProvider"));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    protected RandomReducts getRandomReducts(Subtable subtable) throws PropertyConfigurationException {
        return hasOption("dontDiscretize") ?
                new RandomReducts(
                        subtable,
                        getReductsProviderClass(),
                        getIndiscernibilityForMissing(),
                        getDiscernibilityMethod(),
                        getGeneralizedDecisionTransitiveClosure(),
                        getJohnsonReducts()
                ) :
                new RandomReducts(
                        subtable,
                        getReductsProviderClass(),
                        new RsesDiscretizer(
                                new ChiMergeDiscretizationProvider(
                                        getInt("numDiscIntervals", 4),
                                        Double.valueOf(getOption("discSignificance", "0.25"))
                                )
                        ),
                        getIndiscernibilityForMissing(),
                        getDiscernibilityMethod(),
                        getGeneralizedDecisionTransitiveClosure(),
                        getJohnsonReducts()
                );
    }

    protected RandomReducts.IndiscernibilityForMissing getIndiscernibilityForMissing() {
        return hasOption("IndiscernibilityForMissing") ?
                RandomReducts.IndiscernibilityForMissing.valueOf(getOption("IndiscernibilityForMissing")) :
                RandomReducts.IndiscernibilityForMissing.DiscernFromValue;
    }

    protected RandomReducts.DiscernibilityMethod getDiscernibilityMethod() {
        return hasOption("DiscernibilityMethod") ?
                RandomReducts.DiscernibilityMethod.valueOf(getOption("DiscernibilityMethod")) :
                RandomReducts.DiscernibilityMethod.OrdinaryDecisionAndInconsistenciesOmitted;
    }

    protected RandomReducts.GeneralizedDecisionTransitiveClosure getGeneralizedDecisionTransitiveClosure() {
        return hasOption("GeneralizedDecisionTransitiveClosure") ?
                RandomReducts.GeneralizedDecisionTransitiveClosure
                        .valueOf(getOption("GeneralizedDecisionTransitiveClosure")) :
                RandomReducts.GeneralizedDecisionTransitiveClosure.TRUE;
    }

    protected RandomReducts.JohnsonReducts getJohnsonReducts() {
        return hasOption("JohnsonReducts") ?
                RandomReducts.JohnsonReducts.valueOf(getOption("JohnsonReducts")) :
                RandomReducts.JohnsonReducts.All;
    }
}
