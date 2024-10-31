/*-
 * #%L
 * com.oceanbase:obkv-hbase-client
 * %%
 * Copyright (C) 2022 - 2024 OceanBase Group
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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
abstract public class ObTableBuilderBase implements TableBuilder {
    protected TableName tableName;

    protected int       operationTimeout;

    protected int       rpcTimeout;

    protected int       readRpcTimeout;

    protected int       writeRpcTimeout;

    ObTableBuilderBase(TableName tableName, OHConnectionConfiguration ohConnConf) {
        if (tableName == null) {
            throw new IllegalArgumentException("The provided tableName is null");
        }
        this.tableName = tableName;
        this.operationTimeout = tableName.isSystemTable() ? ohConnConf.getMetaOperationTimeout()
            : ohConnConf.getOperationTimeout();
        this.rpcTimeout = ohConnConf.getRpcTimeout();
        this.readRpcTimeout = ohConnConf.getReadRpcTimeout();
        this.writeRpcTimeout = ohConnConf.getWriteRpcTimeout();
    }

    @Override
    public ObTableBuilderBase setOperationTimeout(int timeout) {
        this.operationTimeout = timeout;
        return this;
    }

    @Override
    public ObTableBuilderBase setRpcTimeout(int timeout) {
        this.rpcTimeout = timeout;
        return this;
    }

    @Override
    public ObTableBuilderBase setReadRpcTimeout(int timeout) {
        this.readRpcTimeout = timeout;
        return this;
    }

    @Override
    public ObTableBuilderBase setWriteRpcTimeout(int timeout) {
        this.writeRpcTimeout = timeout;
        return this;
    }

    public TableName getTableName() {
        return this.tableName;
    }

    public int getOperationTimeout() {
        return this.operationTimeout;
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
}
