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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OHDeleteTableExecutor extends AbstractObTableMetaExecutor<Void> {
    private final ObTableClient tableClient;

    OHDeleteTableExecutor(ObTableClient tableClient) {
        this.tableClient = tableClient;
    }

    @Override
    public ObTableRpcMetaType getMetaType() {
        return ObTableRpcMetaType.HTABLE_DELETE_TABLE;
    }

    @Override
    public Void parse(ObTableMetaResponse response) throws IOException {
        // do nothing, error will be thrown from table
        return null;
    }

    public Void deleteTable(String tableName) throws IOException {
        ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        Map<String, Object> requestDataMap = new HashMap<>();
        requestDataMap.put("table_name", tableName);
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonData = objectMapper.writeValueAsString(requestDataMap);
        request.setData(jsonData);
        return execute(tableClient, request);
    }
}
