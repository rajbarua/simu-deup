package com.hazelcast.simudedupe.merchantprofile;

import java.util.Arrays;

/**
 * Fixed-shape merchant-acquiring profile used by the Bank UPI merchant-profile IMap benchmark.
 *
 * <p>The merchant ID is deliberately the IMap key and is not repeated in this value. The payload represents a
 * compiled merchant/payment-routing profile whose exact Compact-serialized size is controlled by the serializer test.
 */
public class MerchantProfile {
    private int profileVersion;
    private long updateRunId;
    private long updatedAtEpochMillis;
    private boolean active;
    private String merchantCategoryCode;
    private String settlementAccountToken;
    private String merchantDisplayName;
    private String terminalProfileId;
    private byte[] profilePayload;

    public MerchantProfile() {
    }

    public MerchantProfile(int profileVersion, long updateRunId, long updatedAtEpochMillis, boolean active,
                                 String merchantCategoryCode, String settlementAccountToken,
                                 String merchantDisplayName, String terminalProfileId, byte[] profilePayload) {
        this.profileVersion = profileVersion;
        this.updateRunId = updateRunId;
        this.updatedAtEpochMillis = updatedAtEpochMillis;
        this.active = active;
        this.merchantCategoryCode = merchantCategoryCode;
        this.settlementAccountToken = settlementAccountToken;
        this.merchantDisplayName = merchantDisplayName;
        this.terminalProfileId = terminalProfileId;
        this.profilePayload = profilePayload;
    }

    public int getProfileVersion() {
        return profileVersion;
    }

    public void setProfileVersion(int profileVersion) {
        this.profileVersion = profileVersion;
    }

    public long getUpdateRunId() {
        return updateRunId;
    }

    public void setUpdateRunId(long updateRunId) {
        this.updateRunId = updateRunId;
    }

    public long getUpdatedAtEpochMillis() {
        return updatedAtEpochMillis;
    }

    public void setUpdatedAtEpochMillis(long updatedAtEpochMillis) {
        this.updatedAtEpochMillis = updatedAtEpochMillis;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getMerchantCategoryCode() {
        return merchantCategoryCode;
    }

    public void setMerchantCategoryCode(String merchantCategoryCode) {
        this.merchantCategoryCode = merchantCategoryCode;
    }

    public String getSettlementAccountToken() {
        return settlementAccountToken;
    }

    public void setSettlementAccountToken(String settlementAccountToken) {
        this.settlementAccountToken = settlementAccountToken;
    }

    public String getMerchantDisplayName() {
        return merchantDisplayName;
    }

    public void setMerchantDisplayName(String merchantDisplayName) {
        this.merchantDisplayName = merchantDisplayName;
    }

    public String getTerminalProfileId() {
        return terminalProfileId;
    }

    public void setTerminalProfileId(String terminalProfileId) {
        this.terminalProfileId = terminalProfileId;
    }

    public byte[] getProfilePayload() {
        return profilePayload;
    }

    public void setProfilePayload(byte[] profilePayload) {
        this.profilePayload = profilePayload;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof MerchantProfile that)) {
            return false;
        }
        return profileVersion == that.profileVersion
                && updateRunId == that.updateRunId
                && updatedAtEpochMillis == that.updatedAtEpochMillis
                && active == that.active
                && java.util.Objects.equals(merchantCategoryCode, that.merchantCategoryCode)
                && java.util.Objects.equals(settlementAccountToken, that.settlementAccountToken)
                && java.util.Objects.equals(merchantDisplayName, that.merchantDisplayName)
                && java.util.Objects.equals(terminalProfileId, that.terminalProfileId)
                && Arrays.equals(profilePayload, that.profilePayload);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(profileVersion, updateRunId, updatedAtEpochMillis, active,
                merchantCategoryCode, settlementAccountToken, merchantDisplayName, terminalProfileId);
        return 31 * result + Arrays.hashCode(profilePayload);
    }
}
