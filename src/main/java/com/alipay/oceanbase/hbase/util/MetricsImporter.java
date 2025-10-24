package com.alipay.oceanbase.hbase.util;

public class MetricsImporter {
    private boolean isFailedOp;
    private long duration;
    private long singleOpCount;

    public MetricsImporter() {
        this.isFailedOp = false;
        this.duration = 0;
        this.singleOpCount = 0;
    }

    public void setIsFailedOp(boolean isFailedOp) {
        this.isFailedOp = isFailedOp;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public void setSingleOpCount(long singleOpCount) {
        this.singleOpCount = singleOpCount;
    }

    public boolean isFailedOp() {
        return isFailedOp;
    }

    public long getDuration() {
        return duration;
    }

    public long getSingleOpCount() {
        return singleOpCount;
    }
}
