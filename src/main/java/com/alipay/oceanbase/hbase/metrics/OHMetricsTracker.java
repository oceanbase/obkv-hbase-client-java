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
        // 使用通用占位类作为 type，表名作为 name，操作类型和指标名组合作为 scope
        // 这样在 JMX 中路径会是：表名 -> 操作类型 -> 指标名
        // newTimer(Class, name, scope) - name 是表名，scope 是 "操作类型.指标名"
        // 生成的路径是: <class>.<表名>.<操作类型>.<指标名>
        Class<?> metricsClass = getMetricsClassForOperation(opType);
        String opTypeName = opType.name();
        this.latencyHistogram = registry.newTimer(OHMetrics.class, opTypeName + ".latencyHistogram", metricsName);
        // newMeter(Class, name, scope, TimeUnit) - name 是表名，scope 是 "操作类型.指标名"
        this.failedOpCounter = registry.newMeter(OHMetrics.class, opTypeName + ".failedOpCounter", metricsName, "failedOpCounter", TimeUnit.MILLISECONDS);
        // newCounter(Class, name, scope) - name 是表名，scope 是 "操作类型.指标名"
        this.totalSingleOpCount = registry.newCounter(OHMetrics.class, opTypeName + ".totalSingleOpCount", metricsName);
        this.totalRuntime = registry.newCounter(OHMetrics.class, opTypeName + ".totalRuntime", metricsName);
    }
    
    
    /**
     * 为每个操作类型创建对应的内部类
     * 这些类用于 JMX 注册，使路径为 com.alipay.oceanbase.hbase.metrics.<OPERATION_TYPE>
     */
    public static class PUT {}
    public static class DELETE {}
    public static class GET {}
    public static class GET_LIST {}
    public static class SCAN {}
    public static class APPEND {}
    public static class INCREMENT {}
    public static class INCREMENT_COLUMN_VALUE {}
    public static class BATCH {}
    public static class BATCH_CALLBACK {}
    public static class PUT_LIST {}
    public static class DELETE_LIST {}
    public static class CHECK_AND_PUT {}
    public static class CHECK_AND_DELETE {}
    public static class CHECK_AND_MUTATE {}
    public static class EXISTS {}
    public static class EXISTS_LIST {}
    public static class MUTATE_ROW {}
    
    /**
     * 通用占位类 - 用于未知的操作类型
     */
    public static class OperationMetricsPlaceholder {}
    
    /**
     * 获取操作类型对应的类名（用于测试和调试）
     * @param opType 操作类型
     * @return 类名
     */
    public static String getClassNameForOperation(OHOperationType opType) {
        Class<?> clazz = getMetricsClassForOperation(opType);
        return clazz.getName();
    }
    
    /**
     * 获取操作类型对应的类（静态方法，供外部调用）
     */
    private static Class<?> getMetricsClassForOperation(OHOperationType opType) {
        switch (opType) {
            case PUT:
                return PUT.class;
            case DELETE:
                return DELETE.class;
            case GET:
                return GET.class;
            case GET_LIST:
                return GET_LIST.class;
            case SCAN:
                return SCAN.class;
            case APPEND:
                return APPEND.class;
            case INCREMENT:
                return INCREMENT.class;
            case INCREMENT_COLUMN_VALUE:
                return INCREMENT_COLUMN_VALUE.class;
            case BATCH:
                return BATCH.class;
            case BATCH_CALLBACK:
                return BATCH_CALLBACK.class;
            case PUT_LIST:
                return PUT_LIST.class;
            case DELETE_LIST:
                return DELETE_LIST.class;
            case CHECK_AND_PUT:
                return CHECK_AND_PUT.class;
            case CHECK_AND_DELETE:
                return CHECK_AND_DELETE.class;
            case CHECK_AND_MUTATE:
                return CHECK_AND_MUTATE.class;
            case EXISTS:
                return EXISTS.class;
            case EXISTS_LIST:
                return EXISTS_LIST.class;
            case MUTATE_ROW:
                return MUTATE_ROW.class;
            default:
                // 对于未知的操作类型，使用通用占位类
                return OperationMetricsPlaceholder.class;
        }
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
        long curTotalCount = this.latencyHistogram.count();
        long curSingleOpCount = this.totalSingleOpCount.count();
        double averageSingleOpCount = curTotalCount == 0 ? 0 : ((double) curSingleOpCount)
                                                               / ((double) curTotalCount); // the average number of single op per request
        long failCount = this.failedOpCounter.count();
        long runtime = this.totalRuntime.count();
        double failRate = this.failedOpCounter.meanRate(); // fail rate

        return MetricsExporter.getInstanceOf(averageSingleOpCount, failRate, failCount, runtime,
            latencyHistogram);
    }
}
