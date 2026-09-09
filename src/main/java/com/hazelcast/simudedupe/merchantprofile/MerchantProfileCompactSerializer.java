package com.hazelcast.simudedupe.merchantprofile;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

/** Compact serializer for the fixed-size UPI merchant profile. */
public final class MerchantProfileCompactSerializer implements CompactSerializer<MerchantProfile> {
    public static final int SERIALIZED_SIZE_BYTES = 10 * 1024;
    public static final int PROFILE_PAYLOAD_SIZE_BYTES = 10_077;
    private static final String TYPE_NAME = "com.hazelcast.simudedupe.merchantprofile.MerchantProfile";

    @Override
    public MerchantProfile read(CompactReader reader) {
        return new MerchantProfile(
                reader.readInt32("profileVersion"),
                reader.readInt64("updateRunId"),
                reader.readInt64("updatedAtEpochMillis"),
                reader.readBoolean("active"),
                reader.readString("merchantCategoryCode"),
                reader.readString("settlementAccountToken"),
                reader.readString("merchantDisplayName"),
                reader.readString("terminalProfileId"),
                reader.readArrayOfInt8("profilePayload"));
    }

    @Override
    public void write(CompactWriter writer, MerchantProfile profile) {
        writer.writeInt32("profileVersion", profile.getProfileVersion());
        writer.writeInt64("updateRunId", profile.getUpdateRunId());
        writer.writeInt64("updatedAtEpochMillis", profile.getUpdatedAtEpochMillis());
        writer.writeBoolean("active", profile.isActive());
        writer.writeString("merchantCategoryCode", profile.getMerchantCategoryCode());
        writer.writeString("settlementAccountToken", profile.getSettlementAccountToken());
        writer.writeString("merchantDisplayName", profile.getMerchantDisplayName());
        writer.writeString("terminalProfileId", profile.getTerminalProfileId());
        writer.writeArrayOfInt8("profilePayload", profile.getProfilePayload());
    }

    @Override
    public String getTypeName() {
        return TYPE_NAME;
    }

    @Override
    public Class<MerchantProfile> getCompactClass() {
        return MerchantProfile.class;
    }
}
