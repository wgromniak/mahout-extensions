package org.mimuw.mahoutattrsel;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FastCutoffPointTest {

    @Test
    public void testCalculateCutoffPoint() throws Exception {

        List<Double> scores = new ArrayList<>();
        scores.add(0,2.0);
        scores.add(1,1.0);
        scores.add(2,3.0);
        scores.add(3,5.0);

        FastCutoffPoint toTest = new FastCutoffPoint();
        assertThat(toTest.calculateCutoffPoint(scores)).isEqualTo(2);
    }

    @Test
    public void testOneElementInScore() throws Exception {

        List<Double> scores = new ArrayList<>();
        scores.add(0,6.77);

        FastCutoffPoint toTest = new FastCutoffPoint();
        assertThat(toTest.calculateCutoffPoint(scores)).isEqualTo(1);
    }
}