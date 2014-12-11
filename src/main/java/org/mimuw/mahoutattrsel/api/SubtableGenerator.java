package org.mimuw.mahoutattrsel.api;

import java.util.List;

/**
 * This is an interface responsible for subtable generation.
 */
public interface SubtableGenerator<T> {

    public List<T> getSubtables();

}
