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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.Size;

import java.io.IOException;
import java.util.*;

public class OHRegionMetricsExecutor extends AbstractObTableMetaExecutor<List<RegionMetrics>> {
    private final ObTableClient tableClient;

    OHRegionMetricsExecutor(ObTableClient tableClient) {
        this.tableClient = tableClient;
    }

    @Override
    public ObTableRpcMetaType getMetaType() throws IOException {
        return ObTableRpcMetaType.HTABLE_REGION_METRICS;
    }

    /*
    * {
        tableName: "tablegroup_name",
        regionList:{
                "regions": [200051, 200052, 200053, 200191, 200192, 200193, ...],
                "memTableSize":[123, 321, 321, 123, 321, 321, ...],
                "ssTableSize":[5122, 4111, 5661, 5122, 4111, 5661, ...]
        }
      }
    * */
    @Override
    public List<RegionMetrics> parse(ObTableMetaResponse response) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode jsonMap = Optional.<JsonNode>ofNullable(objectMapper.readTree(response.getData()))
                .orElseThrow(() -> new IOException("jsonMap is null"));
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
        List<RegionMetrics> metricsList = new ArrayList<>();
        if (regions.isEmpty() || regions.size() != memTableSizeList.size() || memTableSizeList.size() != ssTableSizeList.size()) {
            throw new IOException("size length has to be the same");
        }
        for (int i = 0; i < regions.size(); ++i) {
            String name_str = Integer.toString(regions.get(i));
            byte[] name = name_str.getBytes();
            Size storeFileSize = new Size(((double) ssTableSizeList.get(i)) / (1024 * 1024) , Size.Unit.MEGABYTE); // The unit in original HBase is MEGABYTE, for us it is BYTE
            Size memStoreSize = new Size(((double) memTableSizeList.get(i)) / (1024 * 1024), Size.Unit.MEGABYTE); // The unit in original HBase is MEGABYTE, for us it is BYTE
            OHRegionMetrics ohRegionMetrics = new OHRegionMetrics(tableGroupName, name, storeFileSize, memStoreSize);
            metricsList.add(ohRegionMetrics);
        }
        return metricsList;
    }

    public List<RegionMetrics> getRegionMetrics(String tableName) throws IOException {
        ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        Map<String, Object> requestData = new HashMap<>();
        requestData.put("table_name", tableName);
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonData = objectMapper.writeValueAsString(requestData);
        request.setData(jsonData);
        return execute(tableClient, request);
    }
}
