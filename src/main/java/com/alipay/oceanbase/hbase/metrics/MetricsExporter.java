/*-
 * #%L
 * com.oceanbase:obkv-hbase-client
 * %%
 * Copyright (C) 2022 - 2025 OceanBase Group
 * %%
 * OBKV HBase Client Framework  is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

package com.alipay.oceanbase.hbase.metrics;

import com.yammer.metrics.core.*;
import com.yammer.metrics.stats.Snapshot;

public class MetricsExporter {
    private double   failRate;
    private double   averageSingleOpCount;
    private Timer    latencyHistogram;    // for p99
    private Snapshot latencySnapshot;

    public MetricsExporter() {
        this.failRate = 0;
        this.averageSingleOpCount = 0;
        this.latencyHistogram = null;
        this.latencySnapshot = null;
    }

    public void setFailRate(double failRate) {
        this.failRate = failRate;
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

    public double getAverageOps() {
        return latencyHistogram.meanRate();
    }

    public double getOneMinuteAverageOps() {
        return latencyHistogram.oneMinuteRate();
    }

    public double getFiveMinuteAverageOps() {
        return latencyHistogram.fiveMinuteRate();
    }

    public double getFifteenMinuteAverageOps() {
        return latencyHistogram.fifteenMinuteRate();
    }

    public double getFailRate() {
        return failRate;
    }

    public double getAverageSingleOpCount() {
        return averageSingleOpCount;
    }

    public double getAverageLatency() {
        return latencyHistogram.mean();
    }

    public double getMaxLatency() {
        return latencyHistogram.max();
    }

    public double getMinLatency() {
        return latencyHistogram.min();
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

    public static MetricsExporter getInstanceOf(double averageSingleOpCount, double failRate,
                                                Timer latencyHistogram) {
        MetricsExporter exporter = new MetricsExporter();
        exporter.setAverageSingleOpCount(averageSingleOpCount);
        exporter.setFailRate(failRate);
        exporter.setLatencyHistogram(latencyHistogram);
        exporter.setLatencySnapshot(latencyHistogram.getSnapshot());
        return exporter;
    }
}
