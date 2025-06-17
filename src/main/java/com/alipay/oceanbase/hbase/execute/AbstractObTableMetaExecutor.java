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
        ObTable table = client.getRandomTable();
        ObTableMetaResponse response;
        try {
            response = (ObTableMetaResponse) client.executeWithRetry(table, request, null /*tableName*/
            );
        } catch (Exception e) {
            throw new IOException("Failed to execute request", e);
        }
        return parse(response);
    }
}
