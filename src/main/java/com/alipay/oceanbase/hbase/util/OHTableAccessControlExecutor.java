package com.alipay.oceanbase.hbase.util;

import com.alibaba.fastjson.JSON;
import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OHTableAccessControlExecutor extends AbstractObTableMetaExecutor<Void> {
    private final ObTableClient tableClient;
    private final ObTableRpcMetaType type;

    OHTableAccessControlExecutor(ObTableClient tableClient, ObTableRpcMetaType type) {
        this.tableClient = tableClient;
        this.type = type;
    }

    @Override
    public ObTableRpcMetaType getMetaType() throws IOException {
        return this.type;
    }

    @Override
    public Void parse(ObTableMetaResponse response) throws IOException {
        return null;
    }

    public void enableTable(String tableName) throws IOException, TableNotFoundException, TableNotEnabledException {
        ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        Map<String, Object> requestData = new HashMap<>();
        requestData.put("table_name", tableName);
        String jsonData = JSON.toJSONString(requestData);
        request.setData(jsonData);
        execute(tableClient, request);
    }

    public void disableTable(String tableName) throws IOException, TableNotFoundException, TableNotDisabledException {
        ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        Map<String, Object> requestData = new HashMap<>();
        requestData.put("table_name", tableName);
        String jsonData = JSON.toJSONString(requestData);
        request.setData(jsonData);
        execute(tableClient, request);
    }
}
