/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2024 OceanBase Group
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

import com.alipay.oceanbase.rpc.property.Property;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

import java.util.Properties;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static com.alipay.oceanbase.rpc.property.Property.RPC_CONNECT_TIMEOUT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS_DEFAULT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS_DEFAULT;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.hadoop.hbase.ipc.RpcClient.DEFAULT_SOCKET_TIMEOUT_CONNECT;
import static org.apache.hadoop.hbase.ipc.RpcClient.SOCKET_TIMEOUT_CONNECT;

@InterfaceAudience.Private
public class OHConnectionConfiguration {
    private String           paramUrl;
    private String           database;
    private final Properties properties;
    private final String     fullUsername;
    private final String     password;
    private final String     sysUsername;
    private final String     sysPassword;
    private final String     odpAddr;
    private final int        odpPort;
    private final boolean    odpMode;
    private final long       writeBufferSize;
    private final int        clientOperationTimeout;
    private final int        serverOperationTimeout;
    private final int        metaOperationTimeout;
    private final int        scannerCaching;
    private final long       scannerMaxResultSize;
    private final int        maxKeyValueSize;
    private final int        rpcTimeout;
    private final int        readRpcTimeout;
    private final int        writeRpcTimeout;
    private final int        rpcConnectTimeout;
    private final long       writeBufferPeriodicFlushTimeoutMs;
    private final long       writeBufferPeriodicFlushTimerTickMs;
    private final int        numRetries;

    public OHConnectionConfiguration(Configuration conf) {
        this.paramUrl = conf.get(HBASE_OCEANBASE_PARAM_URL);
        this.fullUsername = conf.get(HBASE_OCEANBASE_FULL_USER_NAME);
        this.password = conf.get(HBASE_OCEANBASE_PASSWORD);
        this.sysUsername = conf.get(HBASE_OCEANBASE_SYS_USER_NAME);
        this.sysPassword = conf.get(HBASE_OCEANBASE_SYS_PASSWORD);
        this.odpAddr = conf.get(HBASE_OCEANBASE_ODP_ADDR);
        this.odpPort = conf.getInt(HBASE_OCEANBASE_ODP_PORT, -1);
        this.odpMode = conf.getBoolean(HBASE_OCEANBASE_ODP_MODE, false);
        String database = conf.get(HBASE_OCEANBASE_DATABASE);
        if (isBlank(database)) {
            database = "default";
        }
        this.database = database;
        this.writeBufferSize = conf.getLong(WRITE_BUFFER_SIZE_KEY, WRITE_BUFFER_SIZE_DEFAULT);
        this.metaOperationTimeout = conf.getInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT,
            HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
        this.clientOperationTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
            HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
        this.serverOperationTimeout = conf.getInt(HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
                HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);
        this.rpcTimeout = conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
            HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
        this.readRpcTimeout = conf.getInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY,
            HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
        this.writeRpcTimeout = conf.getInt(HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY,
            HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
        this.writeBufferPeriodicFlushTimeoutMs = conf.getLong(
            WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS, WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS_DEFAULT);
        this.writeBufferPeriodicFlushTimerTickMs = conf.getLong(
            WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS,
            WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS_DEFAULT);
        int rpcConnectTimeout = -1;
        if (conf.get(SOCKET_TIMEOUT_CONNECT) != null) {
            rpcConnectTimeout = conf.getInt(SOCKET_TIMEOUT_CONNECT, DEFAULT_SOCKET_TIMEOUT_CONNECT);
        } else {
            if (conf.get(SOCKET_TIMEOUT) != null) {
                rpcConnectTimeout = conf.getInt(SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT);
            } else {
                rpcConnectTimeout = RPC_CONNECT_TIMEOUT.getDefaultInt(); // use table default value
            }
        }
        this.rpcConnectTimeout = rpcConnectTimeout;
        this.numRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
        this.scannerCaching = conf.getInt(HConstants.HBASE_CLIENT_SCANNER_CACHING,
            Integer.MAX_VALUE);
        this.scannerMaxResultSize = conf.getLong(
            HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, WRITE_BUFFER_SIZE_DEFAULT);
        this.maxKeyValueSize = conf.getInt(MAX_KEYVALUE_SIZE_KEY, MAX_KEYVALUE_SIZE_DEFAULT);
        properties = new Properties();
        for (Property property : Property.values()) {
            String value = conf.get(property.getKey());
            if (value != null) {
                properties.put(property.getKey(), value);
            }
        }
    }

    public void setParamUrl(String paramUrl) {
        this.paramUrl = paramUrl;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public long getWriteBufferSize() {
        return this.writeBufferSize;
    }

    public int getMetaOperationTimeout() {
        return this.metaOperationTimeout;
    }

    public int getClientOperationTimeout() {
        return this.clientOperationTimeout;
    }

    public int getServerOperationTimeout() {
        return this.serverOperationTimeout;
    }

    public int getScannerCaching() {
        return this.scannerCaching;
    }

    public int getMaxKeyValueSize() {
        return this.maxKeyValueSize;
    }

    public int getRpcTimeout() {
        return this.rpcTimeout;
    }

    public int getReadRpcTimeout() {
        return this.readRpcTimeout;
    }

    public int getWriteRpcTimeout() {
        return this.writeRpcTimeout;
    }

    public int getRpcConnectTimeout() {
        return this.rpcConnectTimeout;
    }

    public long getScannerMaxResultSize() {
        return this.scannerMaxResultSize;
    }

    public Properties getProperties() {
        return this.properties;
    }

    public int getOdpPort() {
        return this.odpPort;
    }

    public String getSysPassword() {
        return this.sysPassword;
    }

    public String getSysUsername() {
        return this.sysUsername;
    }

    public String getPassword() {
        return this.password;
    }

    public String getFullUsername() {
        return this.fullUsername;
    }

    public String getParamUrl() {
        return this.paramUrl;
    }

    public String getOdpAddr() {
        return this.odpAddr;
    }

    public boolean isOdpMode() {
        return this.odpMode;
    }

    public String getDatabase() {
        return this.database;
    }

    public long getWriteBufferPeriodicFlushTimeoutMs() {
        return this.writeBufferPeriodicFlushTimeoutMs;
    }

    public long getWriteBufferPeriodicFlushTimerTickMs() {
        return this.writeBufferPeriodicFlushTimerTickMs;
    }

    public int getNumRetries() {
        return this.numRetries;
    }
}
