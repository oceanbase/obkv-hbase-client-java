package com.alipay.oceanbase.hbase.util;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class MetricsExporter {
    private double failRate;
    private long failCount;
    private long totalRuntime;
    private double averageSingleOpCount;
    private Timer latencyHistogram; // for p99
    private Snapshot latencySnapshot;

    public MetricsExporter() {
        this.failRate = 0;
        this.failCount = 0L;
        this.totalRuntime = 0L;
        this.averageSingleOpCount = 0;
        this.latencyHistogram = null;
        this.latencySnapshot = null;
    }

    public void setFailRate(double failRate) {
        this.failRate = failRate;
    }

    public void setFailCount(long failCount) {
        this.failCount = failCount;
    }

    public void setTotalRuntime(long totalRuntime) {
        this.totalRuntime = totalRuntime;
    }

    public void setAverageSingleOpCount(double averageSingleOpCount) {
        this.averageSingleOpCount = averageSingleOpCount;
    }

    public void setLatencyHistogram(Timer latencyHistogram) {
        this.latencyHistogram = latencyHistogram;
    }

    public void setLatencySnapshot(Snapshot latencySnapshot) {
        this.latencySnapshot = latencySnapshot;
    }

    public long getCount() {
        return latencyHistogram.getCount();
    }

    public double getAverageOps() {
        return latencyHistogram.getMeanRate();
    }

    public double getOneMinuteAverageOps() {
        return latencyHistogram.getOneMinuteRate();
    }

    public double getFiveMinuteAverageOps() {
        return latencyHistogram.getFiveMinuteRate();
    }

    public double getFifteenMinuteAverageOps() {
        return latencyHistogram.getFifteenMinuteRate();
    }

    public double getFailRate() {
        return failRate;
    }

    public long getFailCount() {
        return failCount;
    }

    public long getTotalRuntime() {
        return totalRuntime;
    }

    public double getAverageSingleOpCount() {
        return averageSingleOpCount;
    }

    public double getAverageLatency() {
        return latencySnapshot.getMean();
    }

    public long getMaxLatency() {
        return latencySnapshot.getMax();
    }

    public long getMinLatency() {
        return latencySnapshot.getMin();
    }

    public double getMedian() {
        return latencySnapshot.getMedian();
    }

    public double get75thPercentile() {
        return latencySnapshot.get75thPercentile();
    }

    public double get95thPercentile() {
        return latencySnapshot.get95thPercentile();
    }

    public double get98thPercentile() {
        return latencySnapshot.get98thPercentile();
    }

    public double get99thPercentile() {
        return latencySnapshot.get99thPercentile();
    }

    public double get999thPercentile() {
        return latencySnapshot.get999thPercentile();
    }

    public static MetricsExporter getInstanceOf(double averageSingleOpCount,
                                                double failRate,
                                                long failCount,
                                                long totalRuntime,
                                                Timer latencyHistogram) {
        MetricsExporter exporter = new MetricsExporter();
        exporter.setAverageSingleOpCount(averageSingleOpCount);
        exporter.setFailRate(failRate);
        exporter.setFailCount(failCount);
        exporter.setTotalRuntime(totalRuntime);
        exporter.setLatencyHistogram(latencyHistogram);
        exporter.setLatencySnapshot(latencyHistogram.getSnapshot());
        return exporter;
    }
}
