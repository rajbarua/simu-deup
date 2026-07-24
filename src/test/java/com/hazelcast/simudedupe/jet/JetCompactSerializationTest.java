package com.hazelcast.simudedupe.jet;

import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JetCompactSerializationTest {
    @Test
    void explicitlySerializesEveryJetAndVerificationTypeAsCompact() {
        SerializationConfig config = new SerializationConfig();
        registerSerializers(config);
        assertCompactRoundTrips(config);
    }

    @Test
    void simulatorClientConfigurationRegistersTheSameCompactSerializers() throws Exception {
        SerializationConfig config = new XmlClientConfigBuilder(new File("client-hazelcast.xml"))
                .build()
                .getSerializationConfig();
        assertCompactRoundTrips(config);
    }

    @Test
    void databaseCountTaskRejectsSqlIdentifiersAndNonNumericPrefixes() {
        assertThrows(IllegalArgumentException.class,
                () -> new PostgresPrefixCountTask("dedup-postgres", "deduplicate; DROP TABLE deduplicate", "2000001"));
        assertThrows(IllegalArgumentException.class,
                () -> new PostgresPrefixCountTask("dedup-postgres", "deduplicate", "2000001%"));
    }

    private static void assertCompactRoundTrips(SerializationConfig config) {
        SchemaService schemaService = inMemorySchemaService();
        InternalSerializationService serializationService =
                new DefaultSerializationServiceBuilder()
                        .setConfig(config)
                        .setSchemaService(schemaService)
                        .build();
        try {
            DedupRequest request = new DedupRequest(
                    "900000100100100000000001", "200000100100100000000001", false);
            DedupRequest requestCopy = compactRoundTrip(serializationService, request);
            assertEquals(request.getOperationId(), requestCopy.getOperationId());
            assertEquals(request.getPaymentId(), requestCopy.getPaymentId());
            assertEquals(request.isExpectedDuplicate(), requestCopy.isExpectedDuplicate());

            DedupResult result = DedupResult.classify(request, null);
            DedupResult resultCopy = compactRoundTrip(serializationService, result);
            assertEquals(result.getOperationId(), resultCopy.getOperationId());
            assertEquals(result.getPaymentId(), resultCopy.getPaymentId());
            assertEquals(result.getOutcome(), resultCopy.getOutcome());
            assertTrue(resultCopy.isClassificationCorrect());

            OperationIdPrefixPredicate<String> predicate =
                    compactRoundTrip(serializationService, new OperationIdPrefixPredicate<>("9000001"));
            assertTrue(predicate.apply(Map.entry("900000100100100000000001", "included")));
            assertFalse(predicate.apply(Map.entry("900000200100100000000001", "excluded")));

            DedupResultStatsAggregator aggregator = new DedupResultStatsAggregator("9000001");
            aggregator.accumulate(Map.entry(result.getOperationId(), result));
            DedupResultStatsAggregator aggregatorCopy = compactRoundTrip(serializationService, aggregator);
            assertArrayEquals(aggregator.aggregate(), aggregatorCopy.aggregate());

            PostgresPrefixCountTask countTask = compactRoundTrip(serializationService,
                    new PostgresPrefixCountTask("dedup-postgres", "deduplicate", "2000001"));
            assertEquals("dedup-postgres", countTask.getConnectionName());
            assertEquals("deduplicate", countTask.getTableName());
            assertEquals("2000001", countTask.getIdPrefix());
        } finally {
            serializationService.dispose();
        }
    }

    private static void registerSerializers(SerializationConfig config) {
        config.getCompactSerializationConfig()
                .addSerializer(new DedupRequestCompactSerializer())
                .addSerializer(new DedupResultCompactSerializer())
                .addSerializer(new OperationIdPrefixPredicateCompactSerializer())
                .addSerializer(new DedupResultStatsAggregatorCompactSerializer())
                .addSerializer(new PostgresPrefixCountTaskCompactSerializer());
    }

    private static SchemaService inMemorySchemaService() {
        Map<Long, Schema> schemas = new ConcurrentHashMap<>();
        return new SchemaService() {
            @Override
            public Schema get(long schemaId) {
                return schemas.get(schemaId);
            }

            @Override
            public void put(Schema schema) {
                schemas.put(schema.getSchemaId(), schema);
            }

            @Override
            public void putLocal(Schema schema) {
                put(schema);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T> T compactRoundTrip(InternalSerializationService serializationService, T value) {
        Data data = serializationService.toData(value);
        assertTrue(data.isCompact(), () -> value.getClass().getSimpleName() + " was not serialized as Compact");
        return (T) serializationService.toObject(data);
    }
}
