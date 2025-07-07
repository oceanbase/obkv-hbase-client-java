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

import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.HConstants;
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
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode jsonMap = Optional.<JsonNode>ofNullable(objectMapper.readTree(response.getData()))
                .orElseThrow(() -> new IOException("jsonMap is null"));
        JsonNode tableGroupNameNode = Optional.<JsonNode>ofNullable(jsonMap.get("tableName"))
                .orElseThrow(() -> new IOException("tableName is null"));
        String tableGroupName = tableGroupNameNode.asText();
        List<Integer> regions = Optional.<List<Integer>>ofNullable(objectMapper.convertValue(jsonMap.get("regions"), new TypeReference<List<Integer>>() {}))
                .orElseThrow(() -> new IOException("regions is null"));
        List<Long> memTableSizeList = Optional.<List<Long>>ofNullable(objectMapper.convertValue(jsonMap.get("memTableSize"), new TypeReference<List<Long>>() {}))
                .orElseThrow(() -> new IOException("memTableSize is null"));
        List<Long> ssTableSizeList = Optional.<List<Long>>ofNullable(objectMapper.convertValue(jsonMap.get("ssTableSize"), new TypeReference<List<Long>>() {}))
                .orElseThrow(() -> new IOException("ssTableSize is null"));
        List<String> boundaryList = Optional.<List<String>>ofNullable(objectMapper.convertValue(jsonMap.get("boundary"), new TypeReference<List<String>>() {}))
                .orElseThrow(() -> new IOException("boundary is null"));
        boolean isHashLikePartition = boundaryList.stream().allMatch(String::isEmpty);
        boolean isRangeLikePartition = boundaryList.stream().noneMatch(String::isEmpty);
        if (!isRangeLikePartition && !isHashLikePartition) {
            // there are empty string and non-empty string in boundary, which is illegal for ADAPTIVE tablegroup
            throw new ObTableUnexpectedException("tablegroup {" + tableGroupName + "} has tables with different partition types");
        }
        Map<byte[], RegionLoad> regionLoadMap = new HashMap<>();
        if (regions.isEmpty() || regions.size() != memTableSizeList.size()
                || memTableSizeList.size() != ssTableSizeList.size()) {
            throw new IOException("size length has to be the same");
        }
        for (int i = 0; i < regions.size(); ++i) {
            String name_str = Integer.toString(regions.get(i));
            byte[] name = name_str.getBytes();
            OHRegionLoad load = new OHRegionLoad(name, (int) (ssTableSizeList.get(i) / (1024 * 1024)), (int) (memTableSizeList.get(i) / (1024 * 1024)));
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
        final ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        final Map<String, String> requestData = new HashMap<>();
        requestData.put("table_name", tableName);
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonData = objectMapper.writeValueAsString(requestData);
        request.setData(jsonData);
        return execute(client, request);
    }
}
