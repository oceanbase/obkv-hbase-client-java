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
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import org.slf4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages shared MetricRegistry and JmxReporter for metrics aggregation.
 * Multiple OHTable instances operating on the same table will share the same
 * MetricRegistry and JMX MBean, allowing unified metrics collection.
 */
public class OHMetricsRegistryManager {
    private static final Logger logger = TableHBaseLoggerFactory.getLogger(OHMetricsRegistryManager.class);

    // Map from metricsName to RegistryInfo
    private static final ConcurrentHashMap<String, RegistryInfo> registryMap = new ConcurrentHashMap<>();

    private static class RegistryInfo {
        final MetricsRegistry registry;
        final JmxReporter reporter;
        final AtomicInteger refCount;

        RegistryInfo(MetricsRegistry registry, JmxReporter reporter) {
            this.registry = registry;
            this.reporter = reporter;
            this.refCount = new AtomicInteger(1);
        }
    }

    /**
     * Get or create a shared MetricRegistry for the given metricsName.
     * If a registry already exists for this metricsName, it will be reused.
     *
     * @param metricsName the metrics name (typically tableName.database)
     * @return the shared MetricRegistry
     */
    public static MetricsRegistry getOrCreateRegistry(String metricsName) {
        return registryMap.compute(metricsName, (key, existing) -> {
            if (existing != null) {
                existing.refCount.incrementAndGet();
                logger.debug("Reusing existing MetricRegistry for metricsName: {}, refCount: {}",
                        metricsName, existing.refCount.get());
                return existing;
            }

            // Create new registry and reporter
            MetricsRegistry registry = new MetricsRegistry();
            JmxReporter reporter = new JmxReporter(registry);
            reporter.start();

            logger.debug("Created new MetricRegistry and JmxReporter for metricsName: {}", metricsName);
            return new RegistryInfo(registry, reporter);
        }).registry;
    }

    /**
     * Release a reference to the MetricRegistry for the given metricsName.
     * When the refCount reaches 0, the reporter will be stopped and the registry removed.
     *
     * @param metricsName the metrics name
     */
    public static void releaseRegistry(String metricsName) {
        registryMap.computeIfPresent(metricsName, (key, info) -> {
            int newCount = info.refCount.decrementAndGet();
            if (newCount <= 0) {
                logger.debug("Stopping JmxReporter and removing MetricRegistry for metricsName: {}", metricsName);
                try {
                    info.reporter.shutdown();
                } catch (Exception e) {
                    logger.warn("Error stopping JmxReporter for metricsName: {}", metricsName, e);
                }
                return null; // Remove from map
            }
            logger.debug("Decremented refCount for metricsName: {}, new refCount: {}", metricsName, newCount);
            return info;
        });
    }

    /**
     * Get the current reference count for a metricsName.
     * Useful for debugging and monitoring.
     *
     * @param metricsName the metrics name
     * @return the reference count, or 0 if not found
     */
    public static int getRefCount(String metricsName) {
        RegistryInfo info = registryMap.get(metricsName);
        return info != null ? info.refCount.get() : 0;
    }

    /**
     * Clear all registries. Should only be used for testing.
     */
    static void clearAll() {
        for (RegistryInfo info : registryMap.values()) {
            try {
                info.reporter.shutdown();
            } catch (Exception e) {
                logger.warn("Error stopping JmxReporter during clearAll", e);
            }
        }
        registryMap.clear();
    }
}