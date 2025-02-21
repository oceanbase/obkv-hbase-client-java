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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
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
            ") PARTITION BY RANGE COLUMNS(`G`) SUBPARTITION BY KEY(`K`) SUBPARTITIONS 97 (" +
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
            ") PARTITION BY RANGE COLUMNS(`G`) SUBPARTITION BY KEY(`K_PREFIX`) SUBPARTITIONS 97 (" +
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
            ") PARTITIONS 97;";
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
            ") PARTITIONS 97;";
    private static String createTableStmts[] = new String[] {createTableStmt1, createTableStmt2, createTableStmt3, createTableStmt4};

    public static void dropTables() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        for (int i = 0; i < tableNames.length; i++) {
            String stmt = "DROP TABLE IF EXISTS " + tableNames[i] + ";";
            conn.createStatement().execute(stmt);
        }
    }

    public static void createTables() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        for (int i = 0; i < createTableStmts.length; i++) {
            conn.createStatement().execute(createTableStmts[i]);
        }
    }

    public static void truncateTables() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        for (int i = 0; i < tableNames.length; i++) {
            String stmt = "TRUNCATE TABLE " + tableNames[i] + ";";
            conn.createStatement().execute(stmt);
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

            String family = getColumnFamilyName(tableNames[i]);
            String key = "putKey";
            String column1 = "putColumn";
            String value = "value";
            long timestamp = System.currentTimeMillis();
            Put put = new Put(toBytes(key));
            put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(value));
            hTable.put(put);

            hTable.close();
        }
    }
}
