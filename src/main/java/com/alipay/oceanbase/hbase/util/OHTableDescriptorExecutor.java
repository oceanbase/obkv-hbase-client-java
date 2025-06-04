package com.alipay.oceanbase.hbase.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import java.util.Map;
import java.util.Optional;

public class OHTableDescriptorExecutor extends AbstractObTableMetaExecutor<HTableDescriptor> {
    private final String tableName;
    private final ObTableClient client;

    public OHTableDescriptorExecutor(String tableName, ObTableClient client) {
        this.tableName = tableName;
        this.client = client;
    }

    @Override
    public HTableDescriptor parse(ObTableMetaResponse response) throws IOException {
        try {
            final String jsonData = response.getData();
            final JSONObject jsonMap = Optional.<JSONObject>ofNullable(JSON.parseObject(jsonData))
                    .orElseThrow(() -> new IOException("jsonMap is null"));
            /*
            {
              "cfDesc": {
                "cf1": {
                  "TTL":604800 
                },
                "cf2": {
                  "TTL":259200 
                }
              },
              "tbDesc": {
                "name":"test"
              }
            }
             */
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            JSONObject cfDesc = jsonMap.getJSONObject("cfDescs");
            if (cfDesc != null) {
                for (Map.Entry<String, Object> entry : cfDesc.entrySet()) {
                    String cfName = entry.getKey();
                    JSONObject attributes = (JSONObject) entry.getValue();
                    HColumnDescriptor cf = new HColumnDescriptor(cfName);
                    cf.setTimeToLive(attributes.getIntValue("TTL"));
                    tableDescriptor.addFamily(cf);
                }
            } else {
                throw new IOException("cfDesc is null");
            }
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

        final String jsonData = JSON.toJSONString(requestData);
        request.setData(jsonData);

        return execute(client, request);
    }

    public boolean isDisable() throws IOException {
        boolean isDisable = false;
        final ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        final Map<String, String> requestData = new HashMap<>();
        requestData.put("table_name", tableName);

        final String jsonData = JSON.toJSONString(requestData);
        request.setData(jsonData);
        try {
            ObTableMetaResponse response = innerExecute(client, request);
            final String responseData = response.getData();
            final JSONObject jsonMap = Optional.<JSONObject>ofNullable(JSON.parseObject(responseData))
                    .orElseThrow(() -> new IOException("jsonMap is null"));
            JSONObject tbDesc = jsonMap.getJSONObject("tableDesc");
            if (tbDesc != null) {
                String state = tbDesc.getString("state");
                if (state.compareToIgnoreCase("disable") == 0) {
                    isDisable = true;
                } else {
                    isDisable = false;
                }
            }
        } catch (IOException e) {
            throw e;
        }
        return isDisable;
    }

    private ObTableMetaResponse innerExecute(ObTableClient client, ObTableMetaRequest request) throws IOException {
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
