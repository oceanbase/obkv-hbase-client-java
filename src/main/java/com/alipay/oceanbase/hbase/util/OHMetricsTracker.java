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

package com.alipay.oceanbase.hbase.util;

import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.OHOperationType;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.*;

import java.util.concurrent.TimeUnit;

public class OHMetricsTracker {
    private final OHOperationType opType;
    private final Timer           latencyHistogram;
    private final Meter           failedOpCounter;
    private final Counter         totalSingleOpCount; // the number of single mutations or queries
    private final Counter         totalRuntime;

    public OHMetricsTracker(MetricsRegistry registry, String metricsName, OHOperationType opType) {
        this.opType = opType;
        this.latencyHistogram = registry.newTimer(OHMetrics.class, "latencyHistogram", metricsName);
        this.failedOpCounter = registry.newMeter(OHMetrics.class, "failedOpCounter",
            "failedOpCounter", TimeUnit.MILLISECONDS);
        this.totalSingleOpCount = registry.newCounter(OHMetrics.class, "totalSingleOpCount",
            metricsName);
        this.totalRuntime = registry.newCounter(OHMetrics.class, "totalRuntime", metricsName);
    }

    public OHOperationType getOpType() {
        return this.opType;
    }

    public void update(MetricsImporter importer) {
        latencyHistogram.update(importer.getDuration(), TimeUnit.MILLISECONDS);
        if (importer.isFailedOp()) {
            failedOpCounter.mark();
        }
        totalSingleOpCount.inc(importer.getSingleOpCount());
        totalRuntime.inc(importer.getDuration());
    }

    public MetricsExporter acquireMetrics() {
        long curTotalCount = this.latencyHistogram.count();
        long curSingleOpCount = this.totalSingleOpCount.count();
        double averageSingleOpCount = ((double) curSingleOpCount) / ((double) curTotalCount); // the average number of single op per request
        double failRate = this.failedOpCounter.fiveMinuteRate(); // fail rate in 15 minutes

        return MetricsExporter.getInstanceOf(averageSingleOpCount, failRate, latencyHistogram);
    }
}
