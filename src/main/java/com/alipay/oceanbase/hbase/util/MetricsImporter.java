package com.alipay.oceanbase.hbase.util;

public class MetricsImporter {
    private boolean isFailedOp;
    private long duration;
    private long batchSize;

    public MetricsImporter() {
        this.isFailedOp = false;
        this.duration = 0;
        this.batchSize = 0;
    }

    public void setIsFailedOp(boolean isFailedOp) {
        this.isFailedOp = isFailedOp;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isFailedOp() {
        return isFailedOp;
    }

    public long getDuration() {
        return duration;
    }

    public long getBatchSize() {
        return batchSize;
    }
}
