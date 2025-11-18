package com.alipay.oceanbase.hbase.util;

import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.OHOperationType;
import com.codahale.metrics.*;

import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class OHMetricsTracker {
    private final OHOperationType opType;
    private final Timer latencyHistogram;
    private final Meter failedOpCounter;
    private final Counter totalSingleOpCount; // the number of single mutations or queries
    private final Counter totalRuntime;

    public OHMetricsTracker(MetricRegistry registry, String metricsName, OHOperationType opType) {
        String typeName = opType.name();
        this.opType = opType;
        this.latencyHistogram = registry.timer(
                name(OHMetrics.class, typeName, "latencyHistogram", metricsName));
        this.failedOpCounter = registry.meter(
                name(OHMetrics.class, typeName, "failedOpCounter", metricsName));
        this.totalSingleOpCount = registry.counter(
                name(OHMetrics.class, typeName, "totalSingleOpCount", metricsName));
        this.totalRuntime = registry.counter(
                name(OHMetrics.class, typeName, "totalRuntime", metricsName));
    }

    public OHOperationType getOpType() {
        return this.opType;
    }

    public void update(MetricsImporter importer) {
        latencyHistogram.update(importer.getDuration(), TimeUnit.MILLISECONDS);
        if (importer.isFailedOp()) {
            failedOpCounter.mark();
        }
        totalSingleOpCount.inc(importer.getBatchSize());
        totalRuntime.inc(importer.getDuration());
    }

    public MetricsExporter acquireMetrics() {
        long curTotalCount = this.latencyHistogram.getCount();
        long curSingleOpCount = this.totalSingleOpCount.getCount();
        double averageSingleOpCount = curTotalCount == 0 ? 0 : ((double) curSingleOpCount) / ((double) curTotalCount); // the average number of single op per request
        long failCount = this.failedOpCounter.getCount();
        long runtime = this.totalRuntime.getCount();
        double failRate = this.failedOpCounter.getMeanRate(); // fail rate

        return MetricsExporter.getInstanceOf(averageSingleOpCount,
                                             failRate,
                                             failCount,
                                             runtime,
                                             latencyHistogram);
    }
}
