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

import com.fasterxml.jackson.core.type.TypeReference;
import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.RegionLoad;

import java.io.IOException;
import java.util.*;

public class OHRegionLoadExecutor extends AbstractObTableMetaExecutor<Map<byte[], RegionLoad>> {
    private final String        tableName;
    private final ObTableClient client;

    OHRegionLoadExecutor(String tableName, ObTableClient client) {
        this.tableName = tableName;
        this.client = client;
    }

    /**
     * 解析元数据响应, 用户需要重写
     *
     * @param response 元数据响应
     * @return 解析后的元数据对象
     * @throws IOException 如果解析失败
     */
    @Override
    public Map<byte[], RegionLoad> parse(ObTableMetaResponse response) throws IOException {
        Map<byte[], RegionLoad> regionLoadMap = new LinkedHashMap<>();
        // use jackson to parse json
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode jsonMap = Optional.<JsonNode>ofNullable(objectMapper.readTree(response.getData())).orElse(null);
        if (jsonMap == null) {
            throw new IOException("jsonMap is null");
        }
        JsonNode tableGroupNameNode = Optional.<JsonNode>ofNullable(jsonMap.get("tableName"))
                .orElseThrow(() -> new IOException("tableName is null"));
        String tableGroupName = tableGroupNameNode.asText();

        JsonNode regionListNode = Optional.<JsonNode>ofNullable(jsonMap.get("regionList"))
                .orElseThrow(() -> new IOException("regionList is null"));
        List<Integer> regions = Optional.<List<Integer>>ofNullable(objectMapper.convertValue(regionListNode.get("regions"), new TypeReference<List<Integer>>() {}))
                .orElseThrow(() -> new IOException("regions is null"));
        List<Integer> memTableSizeList = Optional.<List<Integer>>ofNullable(objectMapper.convertValue(regionListNode.get("memTableSize"), new TypeReference<List<Integer>>() {}))
                .orElseThrow(() -> new IOException("memTableSize is null"));
        List<Integer> ssTableSizeList = Optional.<List<Integer>>ofNullable(objectMapper.convertValue(regionListNode.get("ssTableSize"), new TypeReference<List<Integer>>() {}))
                .orElseThrow(() -> new IOException("ssTableSize is null"));
        for (int i = 0; i < regions.size(); ++i) {
            String name_str = Integer.toString(regions.get(i));
            byte[] name = name_str.getBytes();
            OHRegionLoad load = new OHRegionLoad(name, ssTableSizeList.get(i) / (1024 * 1024), memTableSizeList.get(i) / (1024 * 1024));
            regionLoadMap.put(name, load);
        }
        return regionLoadMap;
    }

    /**
     * 获取元信息类型, 用户需要重写
     *
     * @return 元信息类型
     */
    @Override
    public ObTableRpcMetaType getMetaType() throws IOException {
        return ObTableRpcMetaType.HTABLE_REGION_METRICS;
    }

    public Map<byte[], RegionLoad> getRegionLoad() throws IOException {
        ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        Map<String, Object> requestData = new HashMap<>();
        requestData.put("table_name", tableName);
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonData = objectMapper.writeValueAsString(requestData);
        request.setData(jsonData);
        return execute(client, request);
    }
}
