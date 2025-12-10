package com.alipay.oceanbase.hbase.metrics;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class MetricsExporter {
    private final long totalOpCount;
    private final double averageOps;
    private final double oneMinuteAverageOps;
    private final double fiveMinuteAverageOps;
    private final double fifteenMinuteAverageOps;
    private final double averageLatency; // ms
    private final long maxLatency; // ms
    private final long minLatency; // ms
    private final double medianLatency; // ms
    private final double P75thPercentile; // ms
    private final double P95thPercentile; // ms
    private final double P98thPercentile; // ms
    private final double P99thPercentile; // ms
    private final double P999thPercentile; // ms
    private long failCount;
    private long totalRuntime; // ms
    private double failRate;
    private double averageSingleOpCount;

    public MetricsExporter(Timer latencyHistogram) {
        this.totalOpCount = latencyHistogram.getCount();
        this.averageOps = latencyHistogram.getMeanRate();
        this.oneMinuteAverageOps = latencyHistogram.getOneMinuteRate();
        this.fiveMinuteAverageOps = latencyHistogram.getFiveMinuteRate();
        this.fifteenMinuteAverageOps = latencyHistogram.getFifteenMinuteRate();
        Snapshot snapshot = latencyHistogram.getSnapshot();
        // Time unit of duration stored in Timer is nanosecond, convert it to millisecond
        double nanosecondsToMilliseconds = 1_000_000.0;
        this.averageLatency = snapshot.getMean() / nanosecondsToMilliseconds;
        this.maxLatency = (long) (snapshot.getMax() / nanosecondsToMilliseconds);
        this.minLatency = (long) (snapshot.getMin() / nanosecondsToMilliseconds);
        this.medianLatency = snapshot.getMedian() / nanosecondsToMilliseconds;
        this.P75thPercentile = snapshot.get75thPercentile() / nanosecondsToMilliseconds;
        this.P95thPercentile = snapshot.get95thPercentile() / nanosecondsToMilliseconds;
        this.P98thPercentile = snapshot.get98thPercentile() / nanosecondsToMilliseconds;
        this.P99thPercentile = snapshot.get99thPercentile() / nanosecondsToMilliseconds;
        this.P999thPercentile = snapshot.get999thPercentile() / nanosecondsToMilliseconds;
        this.failRate = 0;
        this.failCount = 0L;
        this.totalRuntime = 0L;
        this.averageSingleOpCount = 0;
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

    public long getCount() {
        return totalOpCount;
    }

    public double getAverageOps() {
        return averageOps;
    }

    public double getOneMinuteAverageOps() {
        return oneMinuteAverageOps;
    }

    public double getFiveMinuteAverageOps() {
        return fiveMinuteAverageOps;
    }

    public double getFifteenMinuteAverageOps() {
        return fifteenMinuteAverageOps;
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
        return averageLatency;
    }

    public long getMaxLatency() {
        return maxLatency;
    }

    public long getMinLatency() {
        return minLatency;
    }

    public double getMedian() {
        return medianLatency;
    }

    public double get75thPercentile() {
        return P75thPercentile;
    }

    public double get95thPercentile() {
        return P95thPercentile;
    }

    public double get98thPercentile() {
        return P98thPercentile;
    }

    public double get99thPercentile() {
        return P99thPercentile;
    }

    public double get999thPercentile() {
        return P999thPercentile;
    }

    public static MetricsExporter getInstanceOf(double averageSingleOpCount,
                                                double failRate,
                                                long failCount,
                                                long totalRuntime,
                                                Timer latencyHistogram) {
        MetricsExporter exporter = new MetricsExporter(latencyHistogram);
        exporter.setAverageSingleOpCount(averageSingleOpCount);
        exporter.setFailRate(failRate);
        exporter.setFailCount(failCount);
        exporter.setTotalRuntime(totalRuntime);
        return exporter;
    }
}
