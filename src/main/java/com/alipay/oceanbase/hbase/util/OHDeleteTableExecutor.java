package com.alipay.oceanbase.hbase.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
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
        requestDataMap.put("name", tableName);
        String jsonData = JSON.toJSONString(requestDataMap);
        request.setData(jsonData);
        return execute(tableClient, request);
    }
}
