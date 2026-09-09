package com.hazelcast.simudedupe.merchantprofile;

import java.util.Random;

/** Creates fixed-width benchmark values outside the timed operation path. */
public final class MerchantProfiles {
    private MerchantProfiles() {
    }

    public static MerchantProfile create(long updateRunId, int sampleId, long updatedAtEpochMillis) {
        byte[] payload = new byte[MerchantProfileCompactSerializer.PROFILE_PAYLOAD_SIZE_BYTES];
        new Random(0x4D45524348414E54L + sampleId).nextBytes(payload);
        return new MerchantProfile(
                1,
                updateRunId,
                updatedAtEpochMillis,
                true,
                "5411",
                String.format("ACCT%028d", sampleId),
                String.format("BANK MERCHANT %018d", sampleId),
                String.format("TERM%020d", sampleId),
                payload);
    }
}
