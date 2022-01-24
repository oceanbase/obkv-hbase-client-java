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

import com.alipay.oceanbase.hbase.OHTableClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class SimpleHBaseClientDemo {
    public static String PARAM_URL      = "";
    public static String FULL_USER_NAME = "";
    public static String PASSWORD       = "";
    public static String SYS_USER_NAME  = "";
    public static String SYS_PASSWORD   = "";

    public static void simpleTest() throws Exception {
        // 1. initial client for table test1
        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_PARAM_URL, PARAM_URL);
        conf.set(HBASE_OCEANBASE_FULL_USER_NAME, FULL_USER_NAME);
        conf.set(HBASE_OCEANBASE_PASSWORD, PASSWORD);
        conf.set(HBASE_OCEANBASE_SYS_USER_NAME, SYS_USER_NAME);
        conf.set(HBASE_OCEANBASE_SYS_PASSWORD, SYS_PASSWORD);
        OHTableClient hTable = new OHTableClient("test1", conf);
        hTable.init();

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
        System.out.printf("column1: " + r.getColumn(family, column));

        // 4. close
        hTable.close();
    }

    public static void main(String[] args) throws Exception {
        simpleTest();
    }
}
