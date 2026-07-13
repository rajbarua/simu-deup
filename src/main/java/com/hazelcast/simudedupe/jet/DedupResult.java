package com.hazelcast.simudedupe.jet;

import java.util.Objects;

/** The business decision emitted by the Jet deduplication job. */
public class DedupResult {
    public enum Outcome {
        ACCEPTED,
        DUPLICATE,
        RETRY
    }

    private final String operationId;
    private final String paymentId;
    private final boolean expectedDuplicate;
    private final Outcome outcome;

    private DedupResult(DedupRequest request, Outcome outcome) {
        this(request.getOperationId(), request.getPaymentId(), request.isExpectedDuplicate(), outcome);
    }

    DedupResult(String operationId, String paymentId, boolean expectedDuplicate, Outcome outcome) {
        this.operationId = Objects.requireNonNull(operationId, "operationId");
        this.paymentId = Objects.requireNonNull(paymentId, "paymentId");
        this.expectedDuplicate = expectedDuplicate;
        this.outcome = Objects.requireNonNull(outcome, "outcome");
    }

    public static DedupResult classify(DedupRequest request, String previousOperationId) {
        Outcome outcome = previousOperationId == null
                ? Outcome.ACCEPTED
                : previousOperationId.equals(request.getOperationId())
                ? Outcome.RETRY
                : Outcome.DUPLICATE;
        return new DedupResult(request, outcome);
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

    public Outcome getOutcome() {
        return outcome;
    }

    public boolean isBusinessDuplicate() {
        return outcome == Outcome.DUPLICATE;
    }

    public boolean isClassificationCorrect() {
        return expectedDuplicate == isBusinessDuplicate();
    }

    @Override
    public String toString() {
        return "DedupResult{" +
                "operationId='" + operationId + '\'' +
                ", paymentId='" + paymentId + '\'' +
                ", expectedDuplicate=" + expectedDuplicate +
                ", outcome=" + outcome +
                '}';
    }
}
