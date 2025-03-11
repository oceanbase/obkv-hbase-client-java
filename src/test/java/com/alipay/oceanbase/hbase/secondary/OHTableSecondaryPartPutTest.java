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
import org.apache.hadoop.hbase.client.Put;
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

public class OHTableSecondaryPartPutTest {
    private static List<String> tableNames = new LinkedList<String>();
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

    public static void testPutImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column = "putColumn";
        String value = "value";
        long timestamp = System.currentTimeMillis();
        Put put = new Put(toBytes(key));
        put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
        hTable.put(put);

        hTable.close();
    }

    public static void testMultiCFPutImpl(Map.Entry<String, List<String>> entry) throws Exception {
        String key = "putKey";
        String value = "value";
        String column = "putColumn";
        long timestamp = System.currentTimeMillis();

        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(entry.getKey());
        hTable.init();
        Put put = new Put(toBytes(key));
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
        }
        hTable.put(put);
        hTable.close();
    }

    @Test
    public void testPut() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartPutTest::testPutImpl);
    }

    @Test
    public void testMultiCFPut() throws Exception {
        FOR_EACH(group2tableNames, OHTableSecondaryPartPutTest::testMultiCFPutImpl);
    }

}
