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

import com.alipay.oceanbase.hbase.util.TableHBaseLoggerFactory;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.OHOperationType;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import org.slf4j.Logger;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class OHMetrics {
    private static final Logger logger = TableHBaseLoggerFactory.getLogger(OHMetrics.class);
    private final String metricsName;
    private final MetricsRegistry registry;
    private final JmxReporter reporter;
    private static OHMetricsTracker[] trackers;
    private final ConcurrentLinkedQueue<ObPair<OHOperationType, MetricsImporter>> metricsQueue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public OHMetrics(String metricsName) {
        this.metricsName = metricsName;
        this.registry = new MetricsRegistry();
        trackers = new OHMetricsTracker[OHOperationType.values().length - 1];
        // OHOperationType(0) is INVALID, skip it
        for (int i = 1; i <= trackers.length; ++i) {
            OHOperationType opType = OHOperationType.valueOf(i);
            trackers[i - 1] = new OHMetricsTracker(this.registry,
                    metricsName,
                    opType);
        }
        this.reporter = new JmxReporter(registry);
        this.reporter.start();
        scheduler.scheduleWithFixedDelay(this::updateMetrics, 0, 10, TimeUnit.SECONDS);
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
        return trackers[opType.getValue() - 1];
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

    public void stop() {
        reporter.shutdown();
        try {
            scheduler.shutdown();
            // wait at most 500 ms to close the scheduler
            if (!scheduler.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.warn("scheduler await for terminate interrupted: {}.", e.getMessage());
            scheduler.shutdownNow();
        }
    }
}
