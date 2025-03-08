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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;

public class OHTableSecondaryPartTest {
    private static List<String> tableNames = new LinkedList<String>();

    public static void dropTables() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        for (int i = 0; i < tableNames.size(); i++) {
            String stmt = "DROP TABLE IF EXISTS " + tableNames.get(i) + ";";
            conn.createStatement().execute(stmt);
            System.out.println("============= drop table " + tableNames.get(i) + " done =============");
        }
        for (int i = 0; i < tableNames.size(); i++) {
            String stmt = "DROP TABLEGROUP IF EXISTS " + getTableName(tableNames.get(i)) + ";";
            conn.createStatement().execute(stmt);
            System.out.println("============= drop tablegroup " + tableNames.get(i) + " done =============");
        }
    }
    public static void createTables(TableTemplateManager.TableType type, boolean printSql) throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        TimeGenerator.TimeRange timeRange = TimeGenerator.generateTestTimeRange();
        String tableGroup = TableTemplateManager.getTableGroupName(type);
        String tableGroupSql = TableTemplateManager.generateTableGroupSQL(tableGroup);
        conn.createStatement().execute(tableGroupSql);
        String tableName = TableTemplateManager.generateTableName(tableGroup, false, 1);
        String sql = TableTemplateManager.getCreateTableSQL(type, tableName, timeRange);
        conn.createStatement().execute(sql);
        tableNames.add(tableName);
        System.out.println("============= create table: " + tableName + "  table_group: " + getTableName(tableName) + " =============\n" + (printSql ? sql : "") + " \n============= done =============\n");
    }

    public static void truncateTables() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        for (int i = 0; i < tableNames.size(); i++) {
            String stmt = "TRUNCATE TABLE " + tableNames.get(i) + ";";
            conn.createStatement().execute(stmt);
            System.out.println("============= truncate table " + tableNames.get(i) + " done =============");
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
        return result;
    }

    @BeforeClass
    public static void before() throws Exception {
        for (TableTemplateManager.TableType type : TableTemplateManager.TableType.values()) {
            createTables(type, true);
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

    public static void testPut(String tableName) throws Exception {
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
    
    public static void testGet(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column = "putColumn";
        String value = "value";
        Put put = new Put(toBytes(key));
        put.add(family.getBytes(), column.getBytes(), toBytes(value));
        hTable.put(put);
        
        Get get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        Result result = hTable.get(get);
        Cell[] cells = result.rawCells();
        assertEquals(1, cells.length);
        assertEquals(column, Bytes.toString(CellUtil.cloneQualifier(cells[0])));
        assertEquals("value", Bytes.toString(CellUtil.cloneValue(cells[0])));
        System.out.println("get table " + tableName + " done");

        hTable.close();
    }
    
    public static void testIncrement(String tableName) throws Exception {
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

        Increment increment = new Increment(key.getBytes());
        increment.addColumn(family.getBytes(), column1.getBytes(), 1L);
        increment.addColumn(family.getBytes(), column2.getBytes(), 1L);
        hTable.increment(increment);
        
        Get get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        Result result = hTable.get(get);
        Cell[] cells = result.rawCells();
        assertEquals(2, cells.length);
        for (Cell cell : cells) {
            if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(column1)) {
                assertEquals("2", Bytes.toString(CellUtil.cloneValue(cell)));
            } else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(column2)) {
                assertEquals("2", Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        hTable.close();
    }
    
    public static void testAppend(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        String value = "value";
        Append append = new Append(key.getBytes());
        KeyValue kv1 = new KeyValue(key.getBytes(), family.getBytes(), column1.getBytes(), value.getBytes());
        KeyValue kv2 = new KeyValue(key.getBytes(), family.getBytes(), column2.getBytes(), value.getBytes());
        append.add(kv1);
        append.add(kv2);
        hTable.append(append);

        Get get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        Result result = hTable.get(get);
        Cell[] cells = result.rawCells();
        assertEquals(2, cells.length);

        for (Cell cell : cells) {
            if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(column1)) {
                assertEquals("valuevalue", Bytes.toString(CellUtil.cloneValue(cell)));
            } else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(column2)) {
                assertEquals("valuevalue", Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        hTable.close();
    }
    
    public static void testCheckAndMutate(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column = "putColumn";
        String value = "value";
        String newValue = "newValue";
        RowMutations mutations = new RowMutations(key.getBytes());
        Put put = new Put(key.getBytes());
        long timestamp = System.currentTimeMillis();
        put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
        hTable.put(put);

        Put newPut = new Put(key.getBytes());
        newPut.add(family.getBytes(), column.getBytes(), timestamp, toBytes(newValue));
        mutations.add(newPut);
        hTable.checkAndMutate(key.getBytes(), family.getBytes(),
                column.getBytes(), CompareFilter.CompareOp.EQUAL, value.getBytes(),
                mutations);

        hTable.close();
    }
    
    public static void testBatchGet(String tableName) throws Exception {
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

        List<Get> gets = new ArrayList<>();
        Get get1 = new Get(key.getBytes());
        get1.addFamily(family.getBytes());
        gets.add(get1);

        Get get2 = new Get(key.getBytes());
        get2.addColumn(family.getBytes(), column1.getBytes());
        gets.add(get2);

        Get get3 = new Get(key.getBytes());
        get3.addColumn(family.getBytes(), column2.getBytes());
        gets.add(get3);
        
        
        Result[] results = hTable.get(gets);
        for (Result result : results) {
            for (Cell cell : result.listCells()) {
                String Q = Bytes.toString(CellUtil.cloneQualifier(cell));
                String V = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("Column: " + Q + ", Value: " + V);
            }
        }
    }
    
    public static void testScan(String tableName) throws Exception {
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
    public void testPut() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartTest::testPut);
    }

    @Test
    public void testGet() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartTest::testGet);
    }

    @Test
    public void testIncrement() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartTest::testIncrement);
    }

    @Test
    public void testAppend() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartTest::testAppend);
    }

    @Test
    public void testCheckAndMutate() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartTest::testCheckAndMutate);
    }
    
    @Test
    public void testBatchGet() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartTest::testBatchGet);
    }

    @Test
    public void testScan() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartTest::testScan);
    }
}
