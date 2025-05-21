package com.alipay.oceanbase.hbase.util;

import com.alibaba.fastjson.JSON;
import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.location.model.TableEntry;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OHRegionLocatorExecutor extends AbstractObTableMetaExecutor<OHRegionLocator> {
    OHRegionLocatorExecutor(ObTableClient client) {
        this.client = client;
    }
    
    @Override
    public ObTableRpcMetaType getMetaType() {
        return ObTableRpcMetaType.HTABLE_REGION_LOCATOR;
    }

    @Override
    public OHRegionLocator parse(ObTableMetaResponse response) throws IOException {
        try {
            String jsonData = response.getData();
            // process json
            return new OHRegionLocator(null, null);
        } catch (IllegalArgumentException e) {
            throw new IOException("msg", e);
        }
    }

    public OHRegionLocator getRegionLocator(String tableName) throws IOException {
        ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        Map<String, Object> requestData = new HashMap<>();
        requestData.put("table_name", tableName);
        String jsonData = JSON.toJSONString(requestData);
        request.setData(jsonData);

        return execute(client, request);
    }
    
    private final ObTableClient client;
}
