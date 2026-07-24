package com.hazelcast.simudedupe.jet;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.EnterpriseSinks;
import com.hazelcast.jet.pipeline.IMapExtension;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;

import static com.hazelcast.simudedupe.jet.DedupResult.classify;

/** Long-running Jet job that performs MapStore-backed payment deduplication. */
public final class JetDeduplicationJob {
    public static final String DEFAULT_JOB_NAME = "simu-dedup-jet";
    public static final String DEFAULT_INPUT_MAP_NAME = "deduplicate-jet-input";
    public static final String DEFAULT_RESULT_MAP_NAME = "deduplicate-jet-results";
    public static final String DEFAULT_DEDUP_MAP_NAME = "deduplicate";
    public static final int DEFAULT_MAX_CONCURRENT_OPS = 8;
    public static final long DEFAULT_SNAPSHOT_INTERVAL_MILLIS = 10_000L;

    private JetDeduplicationJob() {
    }

    public static void main(String[] args) {
        String inputMapName = argument(args, 0, DEFAULT_INPUT_MAP_NAME);
        String resultMapName = argument(args, 1, DEFAULT_RESULT_MAP_NAME);
        String dedupMapName = argument(args, 2, DEFAULT_DEDUP_MAP_NAME);
        int maxConcurrentOps = Integer.parseInt(argument(args, 3,
                Integer.toString(DEFAULT_MAX_CONCURRENT_OPS)));
        long snapshotIntervalMillis = Long.parseLong(argument(args, 4,
                Long.toString(DEFAULT_SNAPSHOT_INTERVAL_MILLIS)));
        String jobName = argument(args, 5, DEFAULT_JOB_NAME);

        if (maxConcurrentOps <= 0 || snapshotIntervalMillis <= 0L) {
            throw new IllegalArgumentException("Jet concurrency and snapshot interval must be positive");
        }

        HazelcastInstance hazelcast = Hazelcast.bootstrappedInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        jobConfig.setSnapshotIntervalMillis(snapshotIntervalMillis);
        jobConfig.setRequireSnapshotBeforeProcessing(true);
        jobConfig.setSuspendOnFailure(true);
        jobConfig.setStoreMetricsAfterJobCompletion(true);

        // Operator executes this main class through Hazelcast's member-side
        // JAR service. Submission must return after creating the long-running
        // job; join() is explicitly rejected by that service.
        hazelcast.getJet().newJob(
                pipeline(inputMapName, resultMapName, dedupMapName, maxConcurrentOps), jobConfig);
    }

    public static Pipeline pipeline(
            String inputMapName,
            String resultMapName,
            String dedupMapName,
            int maxConcurrentOps
    ) {
        Pipeline pipeline = Pipeline.create();
        StreamStage<DedupRequest> requests = pipeline
                .readFrom(Sources.<String, DedupRequest>mapJournalEntries(
                        inputMapName, JournalInitialPosition.START_FROM_CURRENT))
                .withoutTimestamps()
                .map(event -> {
                    if (event.isAfterLostEvents()) {
                        throw new IllegalStateException("Lost events in input map journal before operation "
                                + event.getKey());
                    }
                    return event.getValue();
                })
                .setName("Read-Dedup-Requests");

        StreamStage<DedupResult> results = requests
                .using(IMapExtension.iMapExtension())
                .maxConcurrentOps(maxConcurrentOps)
                .doNotPreserveOrder()
                .mapUsingPutIfAbsent(
                        dedupMapName,
                        DedupRequest::getPaymentId,
                        DedupRequest::getOperationId,
                        (request, previousOperationId, ignoredNewValue) -> classify(request, previousOperationId))
                .setName("PutIfAbsent-Deduplicate-Payment");

        results.writeTo(EnterpriseSinks.mapFlushSink(dedupMapName, true))
                .setName("Flush-Dedup-Map-On-Snapshot");
        results.writeTo(Sinks.map(resultMapName, DedupResult::getOperationId, result -> result))
                .setName("Write-Dedup-Result");
        return pipeline;
    }

    private static String argument(String[] args, int index, String defaultValue) {
        return args.length > index && !args[index].isBlank() ? args[index] : defaultValue;
    }
}
