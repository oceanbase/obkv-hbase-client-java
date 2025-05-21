package com.alipay.oceanbase.hbase.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.Size;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        List<RegionMetrics> metricsList = new ArrayList<>();
        JSONObject metrcisJSONObject = JSON.parseObject(response.getData());
        String tableGroupName = metrcisJSONObject.getString("tableName");
        JSONObject regionList = metrcisJSONObject.getJSONObject("regionList");
        List<Integer> regions = regionList.getJSONArray("regions").toJavaList(Integer.class);
        List<Integer> memTableSizeList = regionList.getJSONArray("memTableSize").toJavaList(Integer.class);
        List<Integer> ssTableSizeList = regionList.getJSONArray("ssTableSize").toJavaList(Integer.class);
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
        requestData.put("name", tableName);
        String jsonData = JSON.toJSONString(requestData);
        request.setData(jsonData);
        return execute(tableClient, request);
    }
}
