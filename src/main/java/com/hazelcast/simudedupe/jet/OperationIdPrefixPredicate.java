package com.hazelcast.simudedupe.jet;

import com.hazelcast.query.Predicate;

import java.util.Map;
import java.util.Objects;

/** Selects one run or producer from the fixed-width operation-ID keyspace. */
public class OperationIdPrefixPredicate<V> implements Predicate<String, V> {
    private final String prefix;

    public OperationIdPrefixPredicate(String prefix) {
        this.prefix = Objects.requireNonNull(prefix, "prefix");
    }

    @Override
    public boolean apply(Map.Entry<String, V> entry) {
        return entry.getKey() != null && entry.getKey().startsWith(prefix);
    }

    String getPrefix() {
        return prefix;
    }
}
