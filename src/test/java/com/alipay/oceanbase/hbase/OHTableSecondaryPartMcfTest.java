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

package com.alipay.oceanbase.hbase;

import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import com.alipay.oceanbase.hbase.util.TableTemplateManager;
import com.alipay.oceanbase.hbase.util.TimeGenerator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHTableSecondaryPartMcfTest {
    private static Map<String, List<String>> tableNames = new LinkedHashMap<>();

    public static void dropTables() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        for (Map.Entry<String, List<String >> entry : tableNames.entrySet()) {
            for (String tableName : entry.getValue()) {
                String stmt = "DROP TABLE IF EXISTS " + tableName + ";";
                conn.createStatement().execute(stmt);
                System.out.println("============= drop table " + tableName + " done =============");
            }
            String stmt = "DROP TABLEGROUP IF EXISTS " + entry.getKey() + ";";
            conn.createStatement().execute(stmt);
            System.out.println("============= drop tablegroup " + entry.getKey() + " done =============");
        }
    }
    public static void createMultiCFTables(TableTemplateManager.TableType type, boolean printSql) throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        TimeGenerator.TimeRange timeRange = TimeGenerator.generateTestTimeRange();
        String tableGroup = TableTemplateManager.getTableGroupName(type);
        String tableGroupSql = TableTemplateManager.generateTableGroupSQL(tableGroup);
        conn.createStatement().execute(tableGroupSql);
        tableNames.put(tableGroup, new LinkedList<>());
        for (int i = 1; i <= 3; ++i) {
            String tableName = TableTemplateManager.generateTableName(tableGroup, true, i);
            String sql = TableTemplateManager.getCreateTableSQL(type, tableName, timeRange);
            conn.createStatement().execute(sql);
            tableNames.get(tableGroup).add(tableName);
            System.out.println("============= create table: " + tableName 
                                + "  table_group: " + getTableName(tableName) + " =============\n" 
                                + (printSql ? sql : "") + " \n============= done =============\n");
        }
    }
    

    public static void truncateTables() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        for (Map.Entry<String, List<String >> entry : tableNames.entrySet()) {
            for (String tableName : entry.getValue()) {
                String stmt = "TRUNCATE TABLE " + tableName + ";";
                conn.createStatement().execute(stmt);
                System.out.println("============= truncate table " + tableName + " done =============");
            }
        }
    }

    public static String getTableName(String input) throws Exception {
        // 查找 '$' 的索引
        int index = input.indexOf('$');
        // 如果找到了 '$'，提取其前面的部分
        String result;
        if (index != -1) {
            result = input.substring(0, index); // 提取从开始到 '$' 的部分
        } else {
            result = input; // 如果没有 '$' 则返回原字符串
        }
        System.out.println("getTableName:" + result);
        return result;
    }

    public static String getColumnFamilyName(String input) throws Exception {
        // 查找 '$' 的索引
        int index = input.indexOf('$');
        // 如果找到了 '$'，提取其后面的部分
        String result;
        if (index != -1 && index + 1 < input.length()) {
            result = input.substring(index + 1); // 提取从 '$' 后一个字符到结束的部分
        } else {
            result = ""; // 如果没有 '$' 或 '$' 是最后一个字符，则返回空字符串
        }
        System.out.println("getColumnFamilyName:" + result);
        return result;
    }

    @BeforeClass
    public static void before() throws Exception {
        for (TableTemplateManager.TableType type : TableTemplateManager.TableType.values()) {
            createMultiCFTables(type, true);
        }
    }

    @AfterClass
    public static void finish() throws Exception {
        dropTables();
    }

    @Before
    public void prepareCase() throws Exception {
        truncateTables();
    }

    public static void testMultiCFPut(Map.Entry<String, List<String>> entry) throws Exception {
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
    
    public static void testMultiCFGet(Map.Entry<String, List<String>> entry) throws Exception {
        String key = "putKey";
        String column = "putColumn";
        
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            String value = family + "_value";
            long timestamp = System.currentTimeMillis();

            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
            hTable.init();
            Put put = new Put(toBytes(key));
            put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
            hTable.put(put);

            Get get = new Get(key.getBytes());
            get.addFamily(family.getBytes());
            Result r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);

            hTable.close();
        }
    }
    
    @Test
    public void testPut() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartMcfTest::testMultiCFPut);
    }
    
    @Test
    public void testGet() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartMcfTest::testMultiCFGet);
    }
}
