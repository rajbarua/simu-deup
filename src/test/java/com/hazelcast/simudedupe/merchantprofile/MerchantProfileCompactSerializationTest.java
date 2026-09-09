package com.hazelcast.simudedupe.merchantprofile;

import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.RoutingMode;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import com.hazelcast.jet.json.JsonUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MerchantProfileCompactSerializationTest {
    @Test
    void valueIsExactlyTenKibibytesWithExplicitCompactSerializer() {
        SerializationConfig config = new SerializationConfig();
        registerSerializers(config);
        assertRoundTripsAtExactSize(config);
    }

    @Test
    void simulatorClientRegistersMerchantProfileSerializers() throws Exception {
        ClientConfig clientConfig = new XmlClientConfigBuilder(new File("client-hazelcast.xml")).build();
        assertEquals(RoutingMode.ALL_MEMBERS,
                clientConfig.getNetworkConfig().getClusterRoutingConfig().getRoutingMode());
        assertEquals(3, clientConfig.getNetworkConfig().getAddresses().size());
        SerializationConfig config = clientConfig.getSerializationConfig();
        assertRoundTripsAtExactSize(config);

        InternalSerializationService service = serializationService(config);
        try {
            PostgresRunMarkerCountTask copy = compactRoundTrip(service,
                    new PostgresRunMarkerCountTask("dedup-postgres", "merchant_profile", 7L));
            assertEquals("dedup-postgres", copy.getConnectionName());
            assertEquals("merchant_profile", copy.getTableName());
            assertEquals(7L, copy.getUpdateRunId());
        } finally {
            service.dispose();
        }
    }

    @Test
    void databaseCountTaskRejectsUnsafeSettings() {
        assertThrows(IllegalArgumentException.class,
                () -> new PostgresRunMarkerCountTask("dedup-postgres", "profile; DROP TABLE profile", 1L));
        assertThrows(IllegalArgumentException.class,
                () -> new PostgresRunMarkerCountTask("dedup-postgres", "merchant_profile", 0L));
    }

    @Test
    void mapStoreJsonRoundTripPreservesTheCompactDomainValue() throws Exception {
        MerchantProfile profile = MerchantProfiles.create(9L, 17, 1_785_700_000_000L);
        assertEquals(profile, JsonUtil.beanFrom(JsonUtil.toJson(profile), MerchantProfile.class));
    }

    private static void assertRoundTripsAtExactSize(SerializationConfig config) {
        InternalSerializationService service = serializationService(config);
        try {
            MerchantProfile profile = MerchantProfiles.create(7L, 42, 1_785_700_000_000L);
            Data data = service.toData(profile);
            assertTrue(data.isCompact());
            assertEquals(MerchantProfileCompactSerializer.SERIALIZED_SIZE_BYTES, data.totalSize(),
                    "Adjust PROFILE_PAYLOAD_SIZE_BYTES so the complete Compact Data value is exactly 10 KiB");
            assertEquals(profile, service.toObject(data));
        } finally {
            service.dispose();
        }
    }

    private static InternalSerializationService serializationService(SerializationConfig config) {
        return new DefaultSerializationServiceBuilder()
                .setConfig(config)
                .setSchemaService(inMemorySchemaService())
                .build();
    }

    private static void registerSerializers(SerializationConfig config) {
        config.getCompactSerializationConfig()
                .addSerializer(new MerchantProfileCompactSerializer())
                .addSerializer(new PostgresRunMarkerCountTaskCompactSerializer());
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
    private static <T> T compactRoundTrip(InternalSerializationService service, T value) {
        Data data = service.toData(value);
        assertTrue(data.isCompact());
        return (T) service.toObject(data);
    }
}
