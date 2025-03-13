/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2025 OceanBase Group
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

package com.alipay.oceanbase.hbase.secondary;

import com.alipay.oceanbase.hbase.OHTableClient;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import com.alipay.oceanbase.hbase.util.TableTemplateManager;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;

public class OHTableSecondaryPartScanTest {
    private static List<String>              tableNames       = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = new LinkedHashMap<>();

    @BeforeClass
    public static void before() throws Exception {
        openDistributedExecute();
        for (TableTemplateManager.TableType type : TableTemplateManager.TableType.values()) {
            createTables(type, tableNames, group2tableNames, true);
        }
    }

    @AfterClass
    public static void finish() throws Exception {
        closeDistributedExecute();
        dropTables(tableNames, group2tableNames);
    }

    @Before
    public void prepareCase() throws Exception {
        truncateTables(tableNames, group2tableNames);
    }

    public static void testScanImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        long timestamp = System.currentTimeMillis();
        Put put = new Put(toBytes(key));
        put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes("1"));
        put.add(family.getBytes(), column2.getBytes(), timestamp, toBytes("1"));
        hTable.put(put);

        Scan scan = new Scan(key.getBytes());
        scan.addFamily(family.getBytes());
        ResultScanner scanner = hTable.getScanner(scan);
        int count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                assertEquals(column1, Bytes.toString(keyValue.getQualifier()));
                assertEquals("1", Bytes.toString(keyValue.getValue()));
                count++;
            }
        }
        assertEquals(1, count);
    }

    @Test
    public void testScan() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartScanTest::testScanImpl);
    }

    @Test
    public void testMultiCFScan() throws Throwable {
        // implement later
    }
}
