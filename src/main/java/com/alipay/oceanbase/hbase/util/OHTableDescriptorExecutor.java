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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;
import com.alipay.oceanbase.rpc.table.ObTable;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class OHTableDescriptorExecutor extends AbstractObTableMetaExecutor<HTableDescriptor> {
    private final String        tableName;
    private final ObTableClient client;

    public OHTableDescriptorExecutor(String tableName, ObTableClient client) {
        this.tableName = tableName;
        this.client = client;
    }

    @Override
    public HTableDescriptor parse(ObTableMetaResponse response) throws IOException {
        try {
            final String jsonData = response.getData();
            final ObjectMapper objectMapper = new ObjectMapper();
            final JsonNode jsonMap = Optional.<JsonNode>ofNullable(objectMapper.readTree(jsonData))
                    .orElseThrow(() -> new IOException("jsonMap is null"));
            /*
            {
              "cfDescs": {
                "cf1": {
                  "TTL":604800
                  "maxVersions": 3
                },
                "cf2": {
                  "TTL":259200
                  "maxVersions": 2
                }
              },
              "tbDesc": {
                "name":"test",
                "state":"disable" ("enable")
              }
            }
             */
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            JsonNode cfDescsNode = Optional.<JsonNode>ofNullable(jsonMap.get("cfDescs"))
                    .orElseThrow(() -> new IOException("cfDesc is null"));
            Stream<Map.Entry<String, JsonNode>> stream = cfDescsNode.propertyStream();
            stream.forEach(entry -> {
                String cfName = entry.getKey();
                JsonNode value = entry.getValue();
                int ttl = value.path("TTL").asInt();
                int maxVersions = value.path("maxVersions").asInt();
                HColumnDescriptor cf = new HColumnDescriptor(cfName);
                cf.setTimeToLive(ttl);
                cf.setMaxVersions(maxVersions);
                tableDescriptor.addFamily(cf);
            });
            return tableDescriptor;
        } catch (IllegalArgumentException e) {
            throw new IOException("Failed to parse response", e);
        }
    }

    @Override
    public ObTableRpcMetaType getMetaType() throws IOException {
        return ObTableRpcMetaType.HTABLE_GET_DESC;
    }

    public HTableDescriptor getTableDescriptor() throws IOException {
        final ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        final Map<String, String> requestData = new HashMap<>();
        requestData.put("table_name", tableName);
        ObjectMapper objectMapper = new ObjectMapper();
        final String jsonData = objectMapper.writeValueAsString(requestData);
        request.setData(jsonData);

        return execute(client, request);
    }

    public boolean isDisable() throws IOException {
        boolean isDisable = false;
        final ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        final Map<String, String> requestData = new HashMap<>();
        requestData.put("table_name", tableName);
        final ObjectMapper objectMapper = new ObjectMapper();
        final String jsonData = objectMapper.writeValueAsString(requestData);
        request.setData(jsonData);
        try {
            ObTableMetaResponse response = innerExecute(client, request);
            final String responseData = response.getData();
            final JsonNode jsonMap = Optional.<JsonNode>ofNullable(objectMapper.readTree(responseData))
                    .orElseThrow(() -> new IOException("jsonMap is null"));
            JsonNode tbDesc = Optional.<JsonNode>ofNullable(jsonMap.get("tableDesc"))
                    .orElseThrow(() -> new IOException("tableDesc is null"));
            String state = tbDesc.get("state").asText();
            if (state.compareToIgnoreCase("disable") == 0) {
                isDisable = true;
            } else {
                isDisable = false;
            }
        } catch (IOException e) {
            throw e;
        }
        return isDisable;
    }

    private ObTableMetaResponse innerExecute(ObTableClient client, ObTableMetaRequest request)
                                                                                              throws IOException {
        if (request.getMetaType() != getMetaType()) {
            throw new IOException("Invalid meta type, expected " + getMetaType());
        }
        ObTable table = client.getRandomTable();
        ObTableMetaResponse response;
        try {
            response = (ObTableMetaResponse) client.executeWithRetry(
                    table,
                    request,
                    null /*tableName*/
            );
        } catch (Exception e) {
            throw new IOException("Failed to execute request", e);
        }
        return response;
    }
}
