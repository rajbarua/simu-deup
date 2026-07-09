package com.hazelcast.simudedupe.store;

import com.hazelcast.jet.json.JsonUtil;

import java.io.IOException;
import java.io.UncheckedIOException;

public class TransactionDeduplicationMapStore extends MetadataAwareMapStore<String> {
    @Override
    protected String serialize(String value) {
        try {
            return JsonUtil.toJson(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected String deserialize(String payload) {
        if (payload == null) {
            return null;
        }
        try {
            return JsonUtil.beanFrom(payload, String.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
