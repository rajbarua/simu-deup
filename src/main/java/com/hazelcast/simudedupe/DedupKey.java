package com.hazelcast.simudedupe;

import java.util.Locale;

public final class DedupKey {
    public static final String DEFAULT_SERVICE = "UPI";
    public static final String DEFAULT_SEPARATOR = "|";

    private DedupKey() {
    }

    public static String key(String serviceName, String paymentId) {
        return normalizeService(serviceName) + DEFAULT_SEPARATOR + paymentId;
    }

    public static String serviceName(String key) {
        int index = key.indexOf(DEFAULT_SEPARATOR);
        if (index < 0) {
            return DEFAULT_SERVICE;
        }
        return key.substring(0, index);
    }

    public static String paymentId(String key) {
        int index = key.indexOf(DEFAULT_SEPARATOR);
        if (index < 0) {
            return key;
        }
        return key.substring(index + DEFAULT_SEPARATOR.length());
    }

    public static String normalizeService(String serviceName) {
        if (serviceName == null || serviceName.isBlank()) {
            return DEFAULT_SERVICE;
        }
        return serviceName.trim().toUpperCase(Locale.ROOT);
    }
}
