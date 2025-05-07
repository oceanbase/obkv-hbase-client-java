/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2022 OceanBase Group
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

package com.oceanbase.example;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class SimpleHBaseClientDemo {
    public static void simpleTest() throws Exception {
        // 1. initial connection for table test1
        HBaseConfiguration conf = new HBaseConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("test1");
        Table hTable = connection.getTable(tableName);

        // 2. put data like hbase
        byte[] family = toBytes("family1");
        byte[] rowKey = toBytes("rowKey1");
        byte[] column = toBytes("column1");
        Put put = new Put(rowKey);
        put.add(family, column, System.currentTimeMillis(), toBytes("value1"));
        hTable.put(put);

        // 3. get data like hbase
        Get get = new Get(rowKey);
        get.addColumn(family, column);
        Result r = hTable.get(get);
        System.out.println("column1: " + r.getColumn(family, column));

        // 4. close
        hTable.close();
        connection.close();
    }

    public static void main(String[] args) throws Exception {
        simpleTest();
    }
}
