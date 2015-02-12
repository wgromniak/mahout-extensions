package org.mimuw.mahoutattrsel.utils;

import com.codahale.metrics.Gauge;

public final class MemoryGauge implements Gauge<Memory> {

    private static final Runtime RUNTIME = Runtime.getRuntime();

    @Override
    public Memory getValue() {
        return new Memory(
                RUNTIME.maxMemory(),
                RUNTIME.totalMemory(),
                RUNTIME.freeMemory()
        );
    }
}
