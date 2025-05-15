package com.alipay.oceanbase.hbase.execute;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.table.ObTable;

import java.io.IOException;

public abstract class AbstractObTableMetaExecutor<T> implements ObTableMetaExecutor<T> {

    @Override
    public T execute(ObTableClient client, ObTableMetaRequest request) throws IOException {
        if (request.getMetaType() != getMetaType()) {
            throw new IOException("Invalid meta type, expected " + getMetaType());
        }
        ObTable table = client.randomTable();
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
        return parse(response);
    }
}
