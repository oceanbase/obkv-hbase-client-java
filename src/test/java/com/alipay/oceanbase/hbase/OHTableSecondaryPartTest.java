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

import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.getConnection;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.*;

public class OHTableSecondaryPartTest {
    private static String tableNames[] = new String[] {"test$cf1", "test$cf2", "test$cf3", "test$cf4"};
    private static String createTableStmt1 = "CREATE TABLE IF NOT EXISTS `" + tableNames[0] + "` (" +
            "  `K` varbinary(1024) NOT NULL," +
            "  `Q` varbinary(1024) NOT NULL," +
            "  `T` bigint(20) NOT NULL," +
            "  `V` varbinary(1024) DEFAULT NULL," +
            "  `G` bigint(20) GENERATED ALWAYS AS (-T)," +
            "  PRIMARY KEY (`K`, `Q`, `T`)" +
            ") PARTITION BY RANGE COLUMNS(`G`) SUBPARTITION BY KEY(`K`) SUBPARTITIONS 3 (" +
            "  PARTITION `p0` VALUES LESS THAN (1728921600000)," +
            "  PARTITION `p1` VALUES LESS THAN (1729008000000)," +
            "  PARTITION `p2` VALUES LESS THAN (1729094400000)," +
            "  PARTITION `p3` VALUES LESS THAN MAXVALUE" +
            ");";
    private static String createTableStmt2 = "CREATE TABLE IF NOT EXISTS `" + tableNames[1] + "` (" +
            "  `K` varbinary(1024) NOT NULL," +
            "  `Q` varbinary(1024) NOT NULL," +
            "  `T` bigint(20) NOT NULL," +
            "  `V` varbinary(1024) DEFAULT NULL," +
            "  `G` bigint(20) GENERATED ALWAYS AS (-T)," +
            "  K_PREFIX varbinary(1024) generated always as (substring(`K`, 1, 4))," +
            "  PRIMARY KEY (`K`, `Q`, `T`)" +
            ") PARTITION BY RANGE COLUMNS(`G`) SUBPARTITION BY KEY(`K_PREFIX`) SUBPARTITIONS 3 (" +
            "  PARTITION `p0` VALUES LESS THAN (1728921600000)," +
            "  PARTITION `p1` VALUES LESS THAN (1729008000000)," +
            "  PARTITION `p2` VALUES LESS THAN (1729094400000)," +
            "  PARTITION `p3` VALUES LESS THAN MAXVALUE" +
            ");";
    private static String createTableStmt3 = "CREATE TABLE IF NOT EXISTS `" + tableNames[2] + "` (" +
            "    `K` varbinary(1024) NOT NULL," +
            "    `Q` varbinary(1024) NOT NULL," +
            "    `T` bigint(20) NOT NULL," +
            "    `V` varbinary(1024) DEFAULT NULL," +
            "    `G` bigint(20) GENERATED ALWAYS AS (-T)," +
            "    PRIMARY KEY (`K`, `Q`, `T`)" +
            ") PARTITION BY KEY(`K`) SUBPARTITION BY RANGE COLUMNS(`G`) SUBPARTITION TEMPLATE (" +
            "    SUBPARTITION `p0` VALUES LESS THAN (1728921600000)," +
            "    SUBPARTITION `p1` VALUES LESS THAN (1729008000000)," +
            "    SUBPARTITION `p2` VALUES LESS THAN (1729094400000)," +
            "    SUBPARTITION `p3` VALUES LESS THAN MAXVALUE" +
            ") PARTITIONS 3;";
    private static String createTableStmt4 = "CREATE TABLE IF NOT EXISTS `" + tableNames[3] + "` (" +
            "  `K` varbinary(1024) NOT NULL," +
            "  `Q` varbinary(1024) NOT NULL," +
            "  `T` bigint(20) NOT NULL," +
            "  `V` varbinary(1024) DEFAULT NULL," +
            "  `G` bigint(20) GENERATED ALWAYS AS (-T)," +
            "  K_PREFIX varbinary(1024) generated always as (substring(`K`, 1, 4))," +
            "  PRIMARY KEY (`K`, `Q`, `T`)" +
            ") PARTITION BY KEY(`K_PREFIX`) SUBPARTITION BY RANGE COLUMNS(`G`) SUBPARTITION TEMPLATE (" +
            "    SUBPARTITION `p0` VALUES LESS THAN (1728921600000)," +
            "    SUBPARTITION `p1` VALUES LESS THAN (1729008000000)," +
            "    SUBPARTITION `p2` VALUES LESS THAN (1729094400000)," +
            "    SUBPARTITION `p3` VALUES LESS THAN MAXVALUE" +
            ") PARTITIONS 3;";
    private static String createTableStmts[] = new String[] {createTableStmt1, createTableStmt2, createTableStmt3, createTableStmt4};

    public static void dropTables() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        for (int i = 0; i < tableNames.length; i++) {
            String stmt = "DROP TABLE IF EXISTS " + tableNames[i] + ";";
            conn.createStatement().execute(stmt);
            System.out.println("drop table " + tableNames[i] + " done");
        }
    }

    public static void createTables() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        for (int i = 0; i < createTableStmts.length; i++) {
            conn.createStatement().execute(createTableStmts[i]);
            System.out.println("create table " + tableNames[i] + " done");
        }
    }

    public static void truncateTables() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        for (int i = 0; i < tableNames.length; i++) {
            String stmt = "TRUNCATE TABLE " + tableNames[i] + ";";
            conn.createStatement().execute(stmt);
            System.out.println("truncate table " + tableNames[i] + " done");
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
        createTables();
    }

    @AfterClass
    public static void finish() throws Exception {
        dropTables();
    }

    @Before
    public void prepareCase() throws Exception {
        truncateTables();
    }

    @Test
    public void testPut() throws Exception {
        for (int i = 0; i < tableNames.length; i++) {
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableNames[i]));
            hTable.init();

            System.out.println("put table " + tableNames[i] + " begin");
            String family = getColumnFamilyName(tableNames[i]);
            String key = "putKey";
            String column = "putColumn";
            String value = "value";
            long timestamp = System.currentTimeMillis();
            Put put = new Put(toBytes(key));
            put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
            hTable.put(put);
            System.out.println("put table " + tableNames[i] + " done");

            hTable.close();
        }
    }

    @Test
    public void testIncrement() throws Exception {
        for (int i = 0; i < tableNames.length; i++) {
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableNames[i]));
            hTable.init();

            System.out.println("increment table " + tableNames[i] + " begin");
            String family = getColumnFamilyName(tableNames[i]);
            String key = "Key";
            String column1 = "Column1";
            String column2 = "Column2";
            Increment increment = new Increment(key.getBytes());
            increment.addColumn(family.getBytes(), column1.getBytes(), 1L);
            increment.addColumn(family.getBytes(), column2.getBytes(), 1L);
            hTable.increment(increment);
            System.out.println("increment table " + tableNames[i] + " done");

            hTable.close();
        }
    }

    @Test
    public void testAppend() throws Exception {
        for (int i = 0; i < tableNames.length; i++) {
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableNames[i]));
            hTable.init();

            System.out.println("append table " + tableNames[i] + " begin");
            String family = getColumnFamilyName(tableNames[i]);
            String key = "Key";
            String column1 = "Column1";
            String column2 = "Column2";
            String value = "app";
            Append append = new Append(key.getBytes());
            KeyValue kv1 = new KeyValue(key.getBytes(), family.getBytes(), column1.getBytes(), value.getBytes());
            KeyValue kv2 = new KeyValue(key.getBytes(), family.getBytes(), column2.getBytes(), value.getBytes());
            append.add(kv1);
            append.add(kv2);
            hTable.append(append);
            System.out.println("append table " + tableNames[i] + " done");

            hTable.close();
        }
    }

    @Test
    public void testCheckAndMutate() throws Exception {
        for (int i = 0; i < tableNames.length; i++) {
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableNames[i]));
            hTable.init();

            System.out.println("checkAndMutate table " + tableNames[i] + " begin");
            String family = getColumnFamilyName(tableNames[i]);
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
            System.out.println("checkAndMutate table " + tableNames[i] + " done");

            hTable.close();
        }
    }

    @Test
    public void testGet() throws Exception {
        for (int i = 0; i < tableNames.length; i++) {
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableNames[i]));
            hTable.init();

            System.out.println("put table " + tableNames[i] + " begin");
            String family = getColumnFamilyName(tableNames[i]);
            String key = "putKey";
            String column = "putColumn";
            String value = "value";
            long timestamp = System.currentTimeMillis();
            Put put = new Put(toBytes(key));
            put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
            hTable.put(put);
            System.out.println("put table " + tableNames[i] + " done");

            System.out.println("get table " + tableNames[i] + " begin");
            Get get = new Get(key.getBytes());
            get.setMaxVersions(Integer.MAX_VALUE);
            get.addColumn(family.getBytes(), column.getBytes());
            Result r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);
            System.out.println("get table " + tableNames[i] + " begin");

            hTable.close();
        }
    }

    @Test
    public void testBatchGet() throws Exception {
        long batchSize = 10;
        for (int i = 0; i < tableNames.length; i++) {
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableNames[i]));
            hTable.init();

            System.out.println("put table " + tableNames[i] + " begin");
            String family = getColumnFamilyName(tableNames[i]);
            String column = "putColumn";
            String value = "value";
            long timestamp = System.currentTimeMillis();
            for (int j = 0; j < batchSize; j++) {
                String key = "putKey" + j;
                Put put = new Put(toBytes(key));
                put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
                hTable.put(put);
            }
            System.out.println("put table " + tableNames[i] + " done");

            System.out.println("get table " + tableNames[i] + " begin");
            List<Get> gets = new ArrayList<>();
            for (int j = 0; j < batchSize; j++) {
                String key = "putKey" + j;
                Get get = new Get(key.getBytes());
                get.setMaxVersions(Integer.MAX_VALUE);
                get.addColumn(family.getBytes(), column.getBytes());
                gets.add(get);
            }
            Result[] results = hTable.get(gets);
            for (Result result : results) {
                for (Cell cell : result.listCells()) {
                    String Q = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String V = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println("Column: " + Q + ", Value: " + V);
                }
            }
            System.out.println("get table " + tableNames[i] + " begin");

            hTable.close();
        }
    }
}
