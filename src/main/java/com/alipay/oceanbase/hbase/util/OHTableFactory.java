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
import org.apache.hadoop.classification.InterfaceAudience;
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
@InterfaceAudience.Private
public class OHTableFactory extends HTableFactory {
    private final ExecutorService threadPool;
    private final OHTablePool     tablePool;

    public OHTableFactory(Configuration conf, OHTablePool tablePool) {
        this(conf, tablePool, OHTable
            .createDefaultThreadPoolExecutor(1, conf.getInt(HBASE_HTABLE_PRIVATE_THREADS_MAX,
                DEFAULT_HBASE_HTABLE_PRIVATE_THREADS_MAX), conf.getLong(
                HBASE_HTABLE_THREAD_KEEP_ALIVE_TIME, DEFAULT_HBASE_HTABLE_THREAD_KEEP_ALIVE_TIME)));
    }

    public OHTableFactory(Configuration conf, OHTablePool tablePool,
                          ExecutorService createTableThreadPool) {
        this.threadPool = createTableThreadPool;
        this.tablePool = tablePool;
    }

    @Override
    public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
        try {
            String tableNameStr = Bytes.toString(tableName);
            tableNameStr = tableNameStr.equals(this.tablePool.getOriginTableName()) ? tableNameStr
                : this.tablePool.getOriginTableName();

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
        byte[] isOdpModeAttr = tablePool.getTableAttribute(tableName, HBASE_OCEANBASE_ODP_MODE);
        if ((isOdpModeAttr != null && Bytes.toBoolean(isOdpModeAttr))
            || (isOdpModeAttr == null && configuration.getBoolean(HBASE_OCEANBASE_ODP_MODE, false))) {
            // set odp mode
            configuration.setBoolean(HBASE_OCEANBASE_ODP_MODE, true);

            // set odp address
            String odpAddr = tablePool.getOdpAddr(tableName);
            if (!odpAddr.isEmpty()) {
                configuration.set(HBASE_OCEANBASE_ODP_ADDR, odpAddr);
            }
            checkArgument(isNotBlank(configuration.get(HBASE_OCEANBASE_ODP_ADDR)),
                HBASE_OCEANBASE_ODP_ADDR + " is blank");

            // set odp port
            int odpPort = tablePool.getOdpPort(tableName);
            if (odpPort != -1) {
                configuration.setInt(HBASE_OCEANBASE_ODP_PORT, odpPort);
            }
            checkArgument(configuration.getInt(HBASE_OCEANBASE_ODP_PORT, -1) >= 0,
                HBASE_OCEANBASE_ODP_PORT + " is invalid");

            // set database name
            String database = tablePool.getDatabase(tableName);
            if (!database.isEmpty()) {
                configuration.set(HBASE_OCEANBASE_DATABASE, database);
            }
            checkArgument(isNotBlank(configuration.get(HBASE_OCEANBASE_DATABASE)),
                HBASE_OCEANBASE_DATABASE + " is blank");
        } else {
            // set ocp mode
            configuration.setBoolean(HBASE_OCEANBASE_ODP_MODE, false);

            // set paramUrl
            String paramUrl = Bytes.toString(tablePool.getTableAttribute(tableName,
                HBASE_OCEANBASE_PARAM_URL));
            if (paramUrl != null) {
                configuration.set(HBASE_OCEANBASE_PARAM_URL, paramUrl);
            }
            checkArgument(isNotBlank(configuration.get(HBASE_OCEANBASE_PARAM_URL)),
                "table [" + tableName + "]" + HBASE_OCEANBASE_PARAM_URL + " is blank");

            // set sys user name
            String sysUserName = Bytes.toString(tablePool.getTableAttribute(tableName,
                HBASE_OCEANBASE_SYS_USER_NAME));
            if (sysUserName != null) {
                configuration.set(HBASE_OCEANBASE_SYS_USER_NAME, sysUserName);
            }
            checkArgument(isNotBlank(configuration.get(HBASE_OCEANBASE_SYS_USER_NAME)),
                "table [" + tableName + "]" + HBASE_OCEANBASE_SYS_USER_NAME + " is blank");

            // set sys password
            String sysPassword = Bytes.toString(tablePool.getTableAttribute(tableName,
                HBASE_OCEANBASE_SYS_PASSWORD));
            if (sysPassword != null) {
                configuration.set(HBASE_OCEANBASE_SYS_PASSWORD, sysPassword);
            }
        }
        // set full user name
        String fullUsername = Bytes.toString(tablePool.getTableAttribute(tableName,
            HBASE_OCEANBASE_FULL_USER_NAME));
        if (fullUsername != null) {
            configuration.set(HBASE_OCEANBASE_FULL_USER_NAME, fullUsername);
        }
        checkArgument(isNotBlank(configuration.get(HBASE_OCEANBASE_FULL_USER_NAME)),
            "table [" + tableName + "]" + HBASE_OCEANBASE_FULL_USER_NAME + " is blank");

        // set password
        String password = Bytes.toString(tablePool.getTableAttribute(tableName,
            HBASE_OCEANBASE_PASSWORD));
        if (password != null) {
            configuration.set(HBASE_OCEANBASE_PASSWORD, password);
        }

        return configuration;
    }

    private Configuration copyConfiguration(Configuration origin) {
        Configuration copy = new Configuration();

        for (Map.Entry<String, String> entry : origin) {
            copy.set(entry.getKey(), entry.getValue());
        }
        return copy;
    }

    /**
     * close the factory resources, example create table threadPool
     */
    public void close() {
        if (threadPool != null && !threadPool.isShutdown()) {
            threadPool.shutdown();
        }
    }
}
