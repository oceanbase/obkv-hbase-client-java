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

package com.alipay.oceanbase.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;

public class OHTableP99Test extends HTableTestBase {
    private static Connection conn = null;
    public static String hbaseDeleteSqlType = "HBASE DELETE";
    public static String hbasePutSqlType = "HBASE PUT";
    public static String hbaseScanSqlType = "HBASE SCAN";
    public static String hbaseAppendSqlType = "HBASE APPEND";
    public static String hbaseIncrementSqlType = "HBASE INCREMENT";
    public static String hbaseCheckAndPutSqlType = "HBASE CHECK AND PUT";
    public static String hbaseCheckAndDelSqlType = "HBASE CHECK AND DELETE";

    private static long getResultCount(String sqlType) throws Exception {
        PreparedStatement ps = conn.prepareStatement("select * from oceanbase.gv$ob_query_response_time_histogram " +
                "where sql_type=" + "\"" +sqlType +"\"");
        ResultSet rs = ps.executeQuery();
        long totalCnt = 0L;
        while (rs.next()) {
            totalCnt += rs.getLong("count");
        }
        ps.close();
        return totalCnt;
    }

    private static void flushHistogram() throws Exception {
        Statement statement = conn.createStatement();
        statement.execute("alter system set query_response_time_flush = true;");
    }

    @Before
    public void before() throws Exception {
        hTable = ObHTableTestUtil.newOHTableClient("test");
        ((OHTableClient) hTable).init();
        conn = ObHTableTestUtil.getConnection();
    }

    @After
    public void after() throws Exception {
        hTable.close();
    }

    @Test
    public void testDeleteP99() throws Exception {
        try {
            String key = "putKey";
            String family = "family1";
            Delete delete = new Delete(key.getBytes());
            delete.deleteFamily(family.getBytes());
            hTable.delete(delete);
            assertEquals(1, getResultCount(hbaseDeleteSqlType));
        } finally {
            flushHistogram();
        }
    }

    @Test
    public void testPutP99() throws Exception {
        try {
            String key = "putKey";
            String family = "family1";
            String column = "putColumn1";
            long timestamp = System.currentTimeMillis();
            String value = "value";
            Put put = new Put(toBytes(key));
            put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
            hTable.put(put);
            assertEquals(1, getResultCount(hbasePutSqlType));
        } finally {
            flushHistogram();
        }
    }

    @Test
    public void testCheckAndDelP99() throws Exception {
        try {
            String key = "checkAndDeleteKey";
            String column = "checkAndDeleteColumn";
            String value = "value";
            String family = "family1";
            Delete delete = new Delete(key.getBytes());
            delete.deleteColumn(family.getBytes(), column.getBytes());

            flushHistogram();
            Thread.sleep(100);
            hTable.checkAndDelete(key.getBytes(), family.getBytes(), column.getBytes(),
                    value.getBytes(), delete);
            assertEquals(1, getResultCount(hbaseCheckAndDelSqlType));
        } finally {
            flushHistogram();
        }
    }

    @Test
    public void testCheckAndPutP99() throws Exception {
        try {
            String key = "checkAndDeleteKey";
            String column = "checkAndDeleteColumn";
            String value = "value";
            String family = "family1";

            flushHistogram();
            Thread.sleep(100);
            Put put = new Put(key.getBytes());
            put.add(family.getBytes(), column.getBytes(), "value1".getBytes());
            hTable.checkAndPut(key.getBytes(), "family1".getBytes(), column.getBytes(),
                    value.getBytes(), put);
            assertEquals(1, getResultCount(hbaseCheckAndPutSqlType));
        } finally {
            flushHistogram();
        }
    }

    @Test
    public void testIncrementP99() throws Exception {
        try {
            String column = "incrementColumn";
            String key = "incrementKey";

            flushHistogram();
            Thread.sleep(100);
            Increment increment = new Increment(key.getBytes());
            increment.addColumn("family1".getBytes(), column.getBytes(), 1);
            hTable.increment(increment);
            assertEquals(1, getResultCount(hbaseIncrementSqlType));
        } finally {
            flushHistogram();
        }
    }

    @Test
    public void testAppendP99() throws Exception {
        try {
            String column = "appendColumn";
            String key = "appendKey";

            flushHistogram();
            Thread.sleep(100);
            Append append = new Append(key.getBytes());
            append.add("family1".getBytes(), column.getBytes(), toBytes("_append"));
            hTable.append(append);
            assertEquals(1, getResultCount(hbaseAppendSqlType));
        } finally {
            flushHistogram();
        }
    }

    @Test
    public void testScanP99() throws Exception {
        try {
            String column = "column";
            String family = "family1";
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            scan.setMaxVersions(1);
            scan.setStartRow(("scannerMultiVersion1").getBytes());
            scan.setStopRow(("scannerMultiVersion9").getBytes());
            List<KeyValue> resultList = new ArrayList<KeyValue>();
            ResultScanner scanner = hTable.getScanner(scan);

            flushHistogram();
            Thread.sleep(100);
            scanner.next();
            assertEquals(1, getResultCount(hbaseScanSqlType));
        } finally {
            flushHistogram();
        }
    }
}
