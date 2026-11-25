package com.raftis.playground;

import java.util.Map;

/** Lets tests in other packages use the playground's metrics parser. */
public final class PlaygroundAccess {
    private PlaygroundAccess() {}

    public static Map<String, Double> parse(String prometheusText) {
        return Playground.parseMetrics(prometheusText);
    }
}
