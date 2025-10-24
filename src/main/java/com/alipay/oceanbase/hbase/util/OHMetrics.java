package com.alipay.oceanbase.hbase.util;

import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.OHOperationType;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class OHMetrics {
    private static final Logger logger = TableHBaseLoggerFactory.getLogger(OHMetrics.class);
    private final String metricsName;
    private final MetricRegistry registry;
    private final JmxReporter reporter;
    private static OHMetricsTracker[] trackers;
    private final ConcurrentLinkedQueue<ObPair<OHOperationType, MetricsImporter>> metricsQueue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public OHMetrics(String metricsName) {
        this.metricsName = metricsName;
        this.registry = new MetricRegistry();
        trackers = new OHMetricsTracker[OHOperationType.values().length];
        for (int i = 0; i < trackers.length; ++i) {
            OHOperationType opType = OHOperationType.valueOf(i);
            trackers[i] = new OHMetricsTracker(this.registry,
                    metricsName,
                    opType);
        }
        this.reporter = JmxReporter.forRegistry(this.registry).build();
        this.reporter.start();
    }
    public void start() {
        scheduler.scheduleWithFixedDelay(this::updateMetrics, 10, 0, TimeUnit.SECONDS);
    }
    // get the size of current queue，only update these metrics to corresponding trackers, ignore those concurrently added metrics
    private void updateMetrics() {
        try {
            long size = metricsQueue.size();
            for (long i = 0; i < size; ++i) {
                ObPair<OHOperationType, MetricsImporter> pair = null;
                if ((pair = metricsQueue.poll()) != null) {
                    OHMetricsTracker tracker = getTracker(pair.getLeft());
                    MetricsImporter importer = pair.getRight();
                    tracker.update(importer);
                } else {
                    break;
                }
            }
        } catch (Exception e) {
            logger.warn("update metrics meets exception", e);
        }
    }

    private OHMetricsTracker getTracker(OHOperationType opType) {
        return trackers[opType.getValue()];
    }

    public String getMetricsName() { return this.metricsName; }

    // add metrics into queue asynchronously
    public void update(ObPair<OHOperationType, MetricsImporter> importerPair) {
        metricsQueue.add(importerPair);
    }
    // OPS，RT，P99，failures
    public MetricsExporter acquireMetrics(OHOperationType opType) {
        OHMetricsTracker tracker = getTracker(opType);
        return tracker.acquireMetrics();
    }
}
