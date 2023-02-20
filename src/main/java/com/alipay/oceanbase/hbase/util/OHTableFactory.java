/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2022 OceanBase Group
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

import com.alipay.oceanbase.hbase.OHTable;
import com.alipay.oceanbase.hbase.OHTablePool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static com.alipay.oceanbase.hbase.util.Preconditions.checkArgument;
import static org.apache.commons.lang.StringUtils.isNotBlank;

/**
 * Factory for creating HTable instances.
 * <p>
 * All created HTable will share the same thread pool.
 * Also support setting table attributes for the created table.
 * e.g. auto flush, write buffer size.
 * <p>
 * Setting the table attributes through the method of {@link OHTablePool}.
 * For example, see {@link OHTablePool#setAutoFlush(String, boolean)}
 */
public class OHTableFactory extends HTableFactory {
    private final ExecutorService threadPool;
    private final OHTablePool     tablePool;
    private final int             maxThreads;
    private final long            keepAliveTime;

    public OHTableFactory(Configuration conf, OHTablePool tablePool) {
        this.maxThreads = conf.getInt(HBASE_HTABLE_PRIVATE_THREADS_MAX,
            DEFAULT_HBASE_HTABLE_PRIVATE_THREADS_MAX);
        this.keepAliveTime = conf.getLong(HBASE_HTABLE_THREAD_KEEP_ALIVE_TIME,
            DEFAULT_HBASE_HTABLE_THREAD_KEEP_ALIVE_TIME);
        this.threadPool = OHTable.createDefaultThreadPoolExecutor(1, maxThreads, keepAliveTime);
        this.tablePool = tablePool;
    }

    @Override
    public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
        try {
            String tableNameStr = Bytes.toString(tableName);

            OHTable ht = new OHTable(adjustConfiguration(copyConfiguration(config), tableNameStr),
                tableName, this.threadPool);

            if (tablePool.getTableAttribute(tableNameStr, HBASE_HTABLE_POOL_AUTO_FLUSH) != null) {
                ht.setAutoFlush(tablePool.getAutoFlush(tableNameStr),
                    tablePool.getClearBufferOnFail(tableNameStr));
            }
            if (tablePool.getTableAttribute(tableNameStr, HBASE_HTABLE_POOL_CLEAR_BUFFER_ON_FAIL) != null) {
                ht.setWriteBufferSize(tablePool.getWriteBufferSize(tableNameStr));
            }
            if (tablePool.getTableAttribute(tableNameStr, HBASE_HTABLE_POOL_OPERATION_TIMEOUT) != null) {
                ht.setOperationTimeout(tablePool.getOperationTimeout(tableNameStr));
            }

            if (tablePool.getTableExtendAttribute(tableNameStr, HBASE_OCEANBASE_BATCH_EXECUTOR) != null) {
                ht.setRuntimeBatchExecutor(tablePool.getRuntimeBatchExecutor(tableNameStr));
            }
            return ht;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private Configuration adjustConfiguration(Configuration configuration, String tableName) {
        if (tablePool.getOdpMode(tableName)) {
            // set odp mode
            String odpAddr = tablePool.getOdpAddr(tableName);
            int odpPort = tablePool.getOdpPort(tableName);
            String database = tablePool.getDatabase(tableName);
            checkArgument(isNotBlank(odpAddr), HBASE_OCEANBASE_ODP_ADDR + " is blank");
            checkArgument(odpPort >= 0, HBASE_OCEANBASE_ODP_PORT + " is invalid");
            checkArgument(isNotBlank(database), HBASE_OCEANBASE_DATABASE + " is blank");
            configuration.set(HBASE_OCEANBASE_ODP_ADDR, odpAddr);
            configuration.setInt(HBASE_OCEANBASE_ODP_PORT, odpPort);
            configuration.setBoolean(HBASE_OCEANBASE_ODP_MODE, true);
            configuration.set(HBASE_OCEANBASE_DATABASE, database);
        } else {
            // set ocp mode
            String paramUrl = Bytes.toString(tablePool.getTableAttribute(tableName,
                HBASE_OCEANBASE_PARAM_URL));
            String sysUserName = Bytes.toString(tablePool.getTableAttribute(tableName,
                HBASE_OCEANBASE_SYS_USER_NAME));
            String sysPassword = Bytes.toString(tablePool.getTableAttribute(tableName,
                HBASE_OCEANBASE_SYS_PASSWORD));
            checkArgument(isNotBlank(paramUrl), "table [" + tableName + "]"
                                                + HBASE_OCEANBASE_PARAM_URL + " is blank");
            checkArgument(isNotBlank(sysUserName), "table [" + tableName + "]"
                                                   + HBASE_OCEANBASE_SYS_USER_NAME + " is blank");
            configuration.set(HBASE_OCEANBASE_PARAM_URL, paramUrl);
            configuration.set(HBASE_OCEANBASE_SYS_USER_NAME, sysUserName);
            configuration.set(HBASE_OCEANBASE_SYS_PASSWORD, sysPassword);
        }
        String fullUsername = Bytes.toString(tablePool.getTableAttribute(tableName,
            HBASE_OCEANBASE_FULL_USER_NAME));
        String password = Bytes.toString(tablePool.getTableAttribute(tableName,
            HBASE_OCEANBASE_PASSWORD));
        checkArgument(isNotBlank(fullUsername), "table [" + tableName + "]"
                                                + HBASE_OCEANBASE_FULL_USER_NAME + " is blank");
        configuration.set(HBASE_OCEANBASE_FULL_USER_NAME, fullUsername);
        configuration.set(HBASE_OCEANBASE_PASSWORD, password);
        return configuration;
    }

    private Configuration copyConfiguration(Configuration origin) {
        Configuration copy = new Configuration();

        for (Map.Entry<String, String> entry : origin) {
            copy.set(entry.getKey(), entry.getValue());
        }
        return copy;
    }
}
