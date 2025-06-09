package com.alipay.oceanbase.hbase.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;
import org.apache.hadoop.hbase.RegionLoad;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OHRegionLoadExecutor extends AbstractObTableMetaExecutor<Map<byte[], RegionLoad>> {
    private final String tableName;
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
        final ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        final Map<String, String> requestData = new HashMap<>();
        requestData.put("table_name", tableName);
        return execute(client, request);
    }
}
