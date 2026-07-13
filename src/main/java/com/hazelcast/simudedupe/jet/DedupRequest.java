package com.hazelcast.simudedupe.jet;

import java.util.Objects;

/** A single uniquely identifiable payment deduplication attempt. */
public class DedupRequest {
    private final String operationId;
    private final String paymentId;
    private final boolean expectedDuplicate;

    public DedupRequest(String operationId, String paymentId, boolean expectedDuplicate) {
        this.operationId = Objects.requireNonNull(operationId, "operationId");
        this.paymentId = Objects.requireNonNull(paymentId, "paymentId");
        this.expectedDuplicate = expectedDuplicate;
    }

    public String getOperationId() {
        return operationId;
    }

    public String getPaymentId() {
        return paymentId;
    }

    public boolean isExpectedDuplicate() {
        return expectedDuplicate;
    }
}
