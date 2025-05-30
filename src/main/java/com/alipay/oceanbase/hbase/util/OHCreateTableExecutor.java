/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2025 OceanBase Group
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

import com.alibaba.fastjson.JSON;
import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;

import java.io.IOException;
import java.util.*;

public class OHCreateTableExecutor extends AbstractObTableMetaExecutor<Void> {
    private final ObTableClient client;

    OHCreateTableExecutor(ObTableClient client) {
        this.client = client;
    }

    @Override
    public ObTableRpcMetaType getMetaType() {
        return ObTableRpcMetaType.HTABLE_CREATE_TABLE;
    }

    @Override
    public Void parse(ObTableMetaResponse response) throws IOException {
        // success, do nothing
        return null;
    }

    public void createTable(TableDescriptor tableDescriptor, byte[][] splitKeys) throws IOException {
        final ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        Map<String, Object> requestData = new HashMap<>();
        requestData.put("htable_name", tableDescriptor.getTableName().getName());
        Map<String, Map<String, Integer>> columnFamilies = new HashMap<>();
        for (ColumnFamilyDescriptor columnDescriptor : tableDescriptor.getColumnFamilies()) {
            Map<String, Integer> columnFamily = new HashMap<>();
            columnFamily.put("ttl", columnDescriptor.getTimeToLive());
            columnFamily.put("max_version", columnDescriptor.getMaxVersions());
            columnFamilies.put(columnDescriptor.getNameAsString(), columnFamily);
        }
        requestData.put("column_families", columnFamilies);
        String jsonData = JSON.toJSONString(requestData);
        request.setData(jsonData);
        execute(client, request);
    }
}
