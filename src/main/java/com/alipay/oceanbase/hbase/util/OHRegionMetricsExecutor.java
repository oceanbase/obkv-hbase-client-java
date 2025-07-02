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

import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Table;

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
        "regions": [200051, 200052, 200053, 200191, 200192, 200193, ...],
        "memTableSize":[123, 321, 321, 123, 321, 321, ...],
        "ssTableSize":[5122, 4111, 5661, 5122, 4111, 5661, ...],
        "boundary":["rowkey1", "rowkey2", "rowkey3", ..., "rowkey100", "rowkey101", "rowkey102", ...]
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
        byte[][] startKeys = new byte[regions.size()][];
        byte[][] endKeys = new byte[regions.size()][];
        if (isHashLikePartition) {
            startKeys[0] = HConstants.EMPTY_BYTE_ARRAY;
            endKeys[0] = HConstants.EMPTY_BYTE_ARRAY;
        } else {
            final List<byte[]> startKeysList = new ArrayList<>();
            final List<byte[]> endKeysList = new ArrayList<>();
            for (int i = 0; i < boundaryList.size(); ++i) {
                if (i == 0) {
                    startKeysList.add(HConstants.EMPTY_BYTE_ARRAY);
                    endKeysList.add(boundaryList.get(i).getBytes());
                } else if (i == boundaryList.size() - 1) {
                    startKeysList.add(boundaryList.get(i - 1).getBytes());
                    endKeysList.add(HConstants.EMPTY_BYTE_ARRAY);
                } else {
                    startKeysList.add(boundaryList.get(i - 1).getBytes());
                    endKeysList.add(boundaryList.get(i).getBytes());
                }
            }
            startKeys = startKeysList.toArray(new byte[0][]);
        }
        List<RegionMetrics> metricsList = new ArrayList<>();
        if (regions.isEmpty() || regions.size() != memTableSizeList.size()
                || memTableSizeList.size() != ssTableSizeList.size()
                || ssTableSizeList.size() != startKeys.length) {
            throw new IOException("size length has to be the same");
        }
        for (int i = 0; i < regions.size(); ++i) {
            byte[] startKey = isHashLikePartition ? startKeys[0] : startKeys[i];
            byte[] name = HRegionInfo.createRegionName(
                    TableName.valueOf(tableGroupName),
                    startKey,
                    regions.get(i),
                    HRegionInfo.DEFAULT_REPLICA_ID, true
            );
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
