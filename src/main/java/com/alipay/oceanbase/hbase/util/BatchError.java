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

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;

import java.util.ArrayList;
import java.util.List;

public class BatchError {
    private final List<Throwable> throwables = new ArrayList<Throwable>();
    private final List<Row>       actions    = new ArrayList<Row>();
    private final List<String>    addresses  = new ArrayList<String>();

    public synchronized void add(Throwable ex, Row row, ServerName serverName) {
        if (row == null) {
            throw new IllegalArgumentException("row cannot be null. location=" + serverName);
        }

        throwables.add(ex);
        actions.add(row);
        addresses.add(serverName != null ? serverName.toString() : "null");
    }

    public boolean hasErrors() {
        return !throwables.isEmpty();
    }

    public synchronized RetriesExhaustedWithDetailsException makeException() {
        return new RetriesExhaustedWithDetailsException(new ArrayList<Throwable>(throwables),
            new ArrayList<Row>(actions), new ArrayList<String>(addresses));
    }

    public synchronized void clear() {
        throwables.clear();
        actions.clear();
        addresses.clear();
    }
}
