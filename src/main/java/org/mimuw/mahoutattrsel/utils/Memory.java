package org.mimuw.mahoutattrsel.utils;

final class Memory {
    private final long maxMem;
    private final long allocMem;
    private final long freeMem;

    public Memory(long maxMem, long allocMem, long freeMem) {
        this.maxMem = maxMem;
        this.allocMem = allocMem;
        this.freeMem = freeMem;
    }

    private long toMB(long bytes) {
        return bytes / (1024 * 1024);
    }

    @Override
    public String toString() {
        return "[maxMem=" + toMB(maxMem) + ":allocMem=" + toMB(allocMem) + ":freeMem=" + toMB(freeMem) + "]";
    }
}
