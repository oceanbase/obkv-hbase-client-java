package com.alipay.oceanbase.hbase.util;

import com.alibaba.fastjson.JSON;
import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
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

    public Void enableTable(String tableName) throws IOException, TableNotFoundException, TableNotEnabledException {
        ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        Map<String, Object> requestData = new HashMap<>();
        requestData.put("table_name", tableName);
        String jsonData = JSON.toJSONString(requestData);
        request.setData(jsonData);
        try {
            return execute(tableClient, request);
        } catch (IOException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ObTableException) {
                ObTableException obEx = (ObTableException) cause;
                int errCode = obEx.getErrorCode();
                if(ResultCodes.OB_KV_TABLE_NOT_ENABLED.errorCode == errCode) {
                    throw new TableNotEnabledException("Table is not enabled: " + tableName + obEx);
                } else if (ResultCodes.OB_TABLEGROUP_NOT_EXIST.errorCode == errCode) {
                    throw new TableNotFoundException("Table not found: " + tableName + obEx);
                }
            }
            throw e;
        }
    }

    public Void disableTable(String tableName) throws IOException, TableNotFoundException, TableNotDisabledException {
        ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        Map<String, Object> requestData = new HashMap<>();
        requestData.put("table_name", tableName);
        String jsonData = JSON.toJSONString(requestData);
        request.setData(jsonData);
        try {
            return execute(tableClient, request);
        } catch (IOException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ObTableException) {
                ObTableException obEx = (ObTableException) cause;
                int errCode = obEx.getErrorCode();
                if(ResultCodes.OB_KV_TABLE_NOT_DISABLED.errorCode == errCode) {
                    throw new TableNotDisabledException("Table is not disabled: " + tableName + obEx);
                } else if (ResultCodes.OB_TABLEGROUP_NOT_EXIST.errorCode == errCode) {
                    throw new TableNotFoundException("Table not found: " + tableName + obEx);
                }
            }
            throw e;
        }
    }
}
