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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OHTableExistsExecutor extends AbstractObTableMetaExecutor<Boolean> {
    private final ObTableClient tableClient;

    OHTableExistsExecutor(ObTableClient tableClient) {
        this.tableClient = tableClient;
    }

    @Override
    public ObTableRpcMetaType getMetaType() throws IOException {
        return ObTableRpcMetaType.HTABLE_EXISTS;
    }

    @Override
    public Boolean parse(ObTableMetaResponse response) throws IOException {
        String jsonData = response.getData();
        JSONObject object = JSONObject.parseObject(jsonData);
        return object.getBoolean("exists");
    }

    public Boolean tableExists(String tableName) throws IOException {
        ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        Map<String, Object> requestData = new HashMap<>();
        requestData.put("table_name", tableName);
        String jsonData = JSON.toJSONString(requestData);
        request.setData(jsonData);
        return execute(tableClient, request);
    }
}
