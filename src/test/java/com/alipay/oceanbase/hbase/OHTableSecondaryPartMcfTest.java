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

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHTableSecondaryPartMcfTest {
    private static String tableGroupNames[] = {
            "test_table_group1",
            "test_table_group2",
            "test_table_group3",
            "test_table_group4",
    };
    private static String tableNames[][] = new String[][] {
            {tableGroupNames[0] + "$cf1", tableGroupNames[0] + "$cf2"},
            {tableGroupNames[1] + "$cf1", tableGroupNames[1] + "$cf2"},
            {tableGroupNames[2] + "$cf1", tableGroupNames[2] + "$cf2"},
            {tableGroupNames[3] + "$cf1", tableGroupNames[3] + "$cf2"}
    };
    private static String createTableStmts[][] = {
            {"CREATE TABLE IF NOT EXISTS `" + tableNames[0][0] + "` (" +
                    "  `K` varbinary(1024) NOT NULL," +
                    "  `Q` varbinary(1024) NOT NULL," +
                    "  `T` bigint(20) NOT NULL," +
                    "  `V` varbinary(1024) DEFAULT NULL," +
                    "  `G` bigint(20) GENERATED ALWAYS AS (-T)," +
                    "  PRIMARY KEY (`K`, `Q`, `T`)" +
                    ") TABLEGROUP = "+ tableGroupNames[0] +" PARTITION BY RANGE COLUMNS(`G`) SUBPARTITION BY KEY(`K`) SUBPARTITIONS 3 (" +
                    "  PARTITION `p0` VALUES LESS THAN (1728921600000)," +
                    "  PARTITION `p1` VALUES LESS THAN (1729008000000)," +
                    "  PARTITION `p2` VALUES LESS THAN (1729094400000)," +
                    "  PARTITION `p3` VALUES LESS THAN MAXVALUE" +
                    ");",
            "CREATE TABLE IF NOT EXISTS `" + tableNames[0][1] + "` (" +
                    "  `K` varbinary(1024) NOT NULL," +
                    "  `Q` varbinary(1024) NOT NULL," +
                    "  `T` bigint(20) NOT NULL," +
                    "  `V` varbinary(1024) DEFAULT NULL," +
                    "  `G` bigint(20) GENERATED ALWAYS AS (-T)," +
                    "  PRIMARY KEY (`K`, `Q`, `T`)" +
                    ") TABLEGROUP = "+ tableGroupNames[0] +" PARTITION BY RANGE COLUMNS(`G`) SUBPARTITION BY KEY(`K`) SUBPARTITIONS 3 (" +
                    "  PARTITION `p0` VALUES LESS THAN (1728921600000)," +
                    "  PARTITION `p1` VALUES LESS THAN (1729008000000)," +
                    "  PARTITION `p2` VALUES LESS THAN (1729094400000)," +
                    "  PARTITION `p3` VALUES LESS THAN MAXVALUE" +
                    ");"
            },
            {"CREATE TABLE IF NOT EXISTS `" + tableNames[1][0] + "` (" +
                    "  `K` varbinary(1024) NOT NULL," +
                    "  `Q` varbinary(1024) NOT NULL," +
                    "  `T` bigint(20) NOT NULL," +
                    "  `V` varbinary(1024) DEFAULT NULL," +
                    "  `G` bigint(20) GENERATED ALWAYS AS (-T)," +
                    "  K_PREFIX varbinary(1024) generated always as (substring(`K`, 1, 4))," +
                    "  PRIMARY KEY (`K`, `Q`, `T`)" +
                    ") TABLEGROUP = "+ tableGroupNames[1] +" PARTITION BY RANGE COLUMNS(`G`) SUBPARTITION BY KEY(`K_PREFIX`) SUBPARTITIONS 3 (" +
                    "  PARTITION `p0` VALUES LESS THAN (1728921600000)," +
                    "  PARTITION `p1` VALUES LESS THAN (1729008000000)," +
                    "  PARTITION `p2` VALUES LESS THAN (1729094400000)," +
                    "  PARTITION `p3` VALUES LESS THAN MAXVALUE" +
                    ");",
            "CREATE TABLE IF NOT EXISTS `" + tableNames[1][1] + "` (" +
                    "  `K` varbinary(1024) NOT NULL," +
                    "  `Q` varbinary(1024) NOT NULL," +
                    "  `T` bigint(20) NOT NULL," +
                    "  `V` varbinary(1024) DEFAULT NULL," +
                    "  `G` bigint(20) GENERATED ALWAYS AS (-T)," +
                    "  K_PREFIX varbinary(1024) generated always as (substring(`K`, 1, 4))," +
                    "  PRIMARY KEY (`K`, `Q`, `T`)" +
                    ") TABLEGROUP = "+ tableGroupNames[1] +" PARTITION BY RANGE COLUMNS(`G`) SUBPARTITION BY KEY(`K_PREFIX`) SUBPARTITIONS 3 (" +
                    "  PARTITION `p0` VALUES LESS THAN (1728921600000)," +
                    "  PARTITION `p1` VALUES LESS THAN (1729008000000)," +
                    "  PARTITION `p2` VALUES LESS THAN (1729094400000)," +
                    "  PARTITION `p3` VALUES LESS THAN MAXVALUE" +
                    ");"

            },
            {"CREATE TABLE IF NOT EXISTS `" + tableNames[2][0] + "` (" +
                    "    `K` varbinary(1024) NOT NULL," +
                    "    `Q` varbinary(1024) NOT NULL," +
                    "    `T` bigint(20) NOT NULL," +
                    "    `V` varbinary(1024) DEFAULT NULL," +
                    "    `G` bigint(20) GENERATED ALWAYS AS (-T)," +
                    "    PRIMARY KEY (`K`, `Q`, `T`)" +
                    ") TABLEGROUP = "+ tableGroupNames[2] +" PARTITION BY KEY(`K`) SUBPARTITION BY RANGE COLUMNS(`G`) SUBPARTITION TEMPLATE (" +
                    "    SUBPARTITION `p0` VALUES LESS THAN (1728921600000)," +
                    "    SUBPARTITION `p1` VALUES LESS THAN (1729008000000)," +
                    "    SUBPARTITION `p2` VALUES LESS THAN (1729094400000)," +
                    "    SUBPARTITION `p3` VALUES LESS THAN MAXVALUE" +
                    ") PARTITIONS 3;",
            "CREATE TABLE IF NOT EXISTS `" + tableNames[2][1] + "` (" +
                    "    `K` varbinary(1024) NOT NULL," +
                    "    `Q` varbinary(1024) NOT NULL," +
                    "    `T` bigint(20) NOT NULL," +
                    "    `V` varbinary(1024) DEFAULT NULL," +
                    "    `G` bigint(20) GENERATED ALWAYS AS (-T)," +
                    "    PRIMARY KEY (`K`, `Q`, `T`)" +
                    ") TABLEGROUP = "+ tableGroupNames[2] +" PARTITION BY KEY(`K`) SUBPARTITION BY RANGE COLUMNS(`G`) SUBPARTITION TEMPLATE (" +
                    "    SUBPARTITION `p0` VALUES LESS THAN (1728921600000)," +
                    "    SUBPARTITION `p1` VALUES LESS THAN (1729008000000)," +
                    "    SUBPARTITION `p2` VALUES LESS THAN (1729094400000)," +
                    "    SUBPARTITION `p3` VALUES LESS THAN MAXVALUE" +
                    ") PARTITIONS 3;"

            },
            {"CREATE TABLE IF NOT EXISTS `" + tableNames[3][0] + "` (" +
                    "  `K` varbinary(1024) NOT NULL," +
                    "  `Q` varbinary(1024) NOT NULL," +
                    "  `T` bigint(20) NOT NULL," +
                    "  `V` varbinary(1024) DEFAULT NULL," +
                    "  `G` bigint(20) GENERATED ALWAYS AS (-T)," +
                    "  K_PREFIX varbinary(1024) generated always as (substring(`K`, 1, 4))," +
                    "  PRIMARY KEY (`K`, `Q`, `T`)" +
                    ") TABLEGROUP = "+ tableGroupNames[3] +" PARTITION BY KEY(`K_PREFIX`) SUBPARTITION BY RANGE COLUMNS(`G`) SUBPARTITION TEMPLATE (" +
                    "    SUBPARTITION `p0` VALUES LESS THAN (1728921600000)," +
                    "    SUBPARTITION `p1` VALUES LESS THAN (1729008000000)," +
                    "    SUBPARTITION `p2` VALUES LESS THAN (1729094400000)," +
                    "    SUBPARTITION `p3` VALUES LESS THAN MAXVALUE" +
                    ") PARTITIONS 3;",
            "CREATE TABLE IF NOT EXISTS `" + tableNames[3][1] + "` (" +
                    "  `K` varbinary(1024) NOT NULL," +
                    "  `Q` varbinary(1024) NOT NULL," +
                    "  `T` bigint(20) NOT NULL," +
                    "  `V` varbinary(1024) DEFAULT NULL," +
                    "  `G` bigint(20) GENERATED ALWAYS AS (-T)," +
                    "  K_PREFIX varbinary(1024) generated always as (substring(`K`, 1, 4))," +
                    "  PRIMARY KEY (`K`, `Q`, `T`)" +
                    ") TABLEGROUP = "+ tableGroupNames[3] +" PARTITION BY KEY(`K_PREFIX`) SUBPARTITION BY RANGE COLUMNS(`G`) SUBPARTITION TEMPLATE (" +
                    "    SUBPARTITION `p0` VALUES LESS THAN (1728921600000)," +
                    "    SUBPARTITION `p1` VALUES LESS THAN (1729008000000)," +
                    "    SUBPARTITION `p2` VALUES LESS THAN (1729094400000)," +
                    "    SUBPARTITION `p3` VALUES LESS THAN MAXVALUE" +
                    ") PARTITIONS 3;"

            }
    };

    public static void createTableGroups() throws Exception {
        System.out.print("create table group start");
        Connection conn = ObHTableTestUtil.getConnection();
        for (int i = 0; i < tableGroupNames.length; i++) {
            System.out.print(".");
            String stmt = "CREATE TABLEGROUP IF NOT EXISTS `"+ tableGroupNames[i] +"` SHARDING = 'ADAPTIVE';";
            conn.createStatement().execute(stmt);
        }
        System.out.println("end");
    }

    public static void dropTableGroups() throws Exception {
        System.out.print("drop table group start");
        Connection conn = ObHTableTestUtil.getConnection();
        for (int i = 0; i < tableGroupNames.length; i++) {
            System.out.print(".");
            String stmt = "DROP TABLEGROUP IF EXISTS " + tableGroupNames[i] + ";";
            conn.createStatement().execute(stmt);
        }
        System.out.println("end");
    }

    public static void dropTables() throws Exception {
        System.out.print("drop table start");
        Connection conn = ObHTableTestUtil.getConnection();
        for (int i = 0; i < tableNames.length; i++) {
            for (int j = 0; j < tableNames[i].length; j++) {
                System.out.print(".");
                String stmt = "DROP TABLE IF EXISTS " + tableNames[i][j] + ";";
                conn.createStatement().execute(stmt);
            }
        }
        System.out.println("end");
    }

    public static void createTables() throws Exception {
        System.out.print("create table start");
        Connection conn = ObHTableTestUtil.getConnection();
        for (int i = 0; i < createTableStmts.length; i++) {
            for (int j = 0; j < createTableStmts[i].length; j++) {
                System.out.print(".");
                conn.createStatement().execute(createTableStmts[i][j]);
            }
        }
        System.out.println("end");
    }

    public static void truncateTables() throws Exception {
        System.out.print("truncate table start");
        Connection conn = ObHTableTestUtil.getConnection();
        for (int i = 0; i < tableNames.length; i++) {
            for (int j = 0; j < tableNames[i].length; j++) {
                System.out.print(".");
                String stmt = "TRUNCATE TABLE " + tableNames[i][j] + ";";
                conn.createStatement().execute(stmt);
            }
        }
        System.out.println("end");
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
        createTableGroups();
        createTables();
    }

    @AfterClass
    public static void finish() throws Exception {
        dropTables();
        dropTableGroups();
    }

    @Before
    public void prepareCase() throws Exception {
        truncateTables();
    }

    @Test
    public void testPut() throws Exception {
        String key = "putKey";
        String value = "value";
        String column = "putColumn";
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < tableNames.length; i++) {
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableNames[i][0]));
            hTable.init();
            Put put = new Put(toBytes(key));
            for (int j = 0; j < tableNames[i].length; j++) {
                String family = getColumnFamilyName(tableNames[i][j]);
                put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
            }
            hTable.put(put);
            hTable.close();
        }
    }
}
