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

package com.alipay.oceanbase.hbase.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLSyntaxErrorException;
import java.util.*;
import java.util.function.Supplier;

public class ObHTableSecondaryPartUtil {
    public static void openDistributedExecute() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        String stmt = "ALTER SYSTEM SET _obkv_enable_distributed_execution = true;";
        conn.createStatement().execute(stmt);
    }

    public static void closeDistributedExecute() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        String stmt = "ALTER SYSTEM SET _obkv_enable_distributed_execution = false;";
        conn.createStatement().execute(stmt);
    }

    public static void createTables(TableTemplateManager.TableType type, List<String> tableNames,
                                    Map<String, List<String>> group2tableNames, boolean printSql)
                                                                                                 throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        // single cf table
        if (tableNames != null) {
            createTables(conn, type, tableNames, printSql);
        }
        // multi cf table
        if (group2tableNames != null) {
            createTables(conn, type, group2tableNames, printSql);
        }
    }

    public static void alterTables(TableTemplateManager.TableType type, List<String> tableNames,
                                   Map<String, List<String>> group2tableNames, boolean printSql)
            throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        // single cf table
        if (tableNames != null) {
            alterTables(conn, type, tableNames, printSql);
        }
        // multi cf table
        if (group2tableNames != null) {
            alterTables(conn, type, group2tableNames, printSql);
        }
    }

    public static void createTables(Connection conn, TableTemplateManager.TableType type,
                                    List<String> tableNames, boolean printSql) throws Exception {
        // create single cf table
        if (tableNames != null) {
            TimeGenerator.TimeRange timeRange = TimeGenerator.generateTestTimeRange();
            String tableGroup = TableTemplateManager.getTableGroupName(type, false);
            String tableGroupSql = TableTemplateManager.generateTableGroupSQL(tableGroup);
            conn.createStatement().execute(tableGroupSql);
            String tableName = TableTemplateManager.generateTableName(tableGroup, false, 1);
            String sql = TableTemplateManager.getCreateTableSQL(type, tableName, timeRange);
            try {
                conn.createStatement().execute(sql);
                System.out.println("============= create table: " + tableName + "  table_group: "
                                   + getTableName(tableName) + " =============\n"
                                   + (printSql ? sql : "")
                                   + " \n============= done =============\n");
            } catch (SQLSyntaxErrorException e) {
                if (!e.getMessage().contains("already exists")) {
                    throw e;
                } else {
                    System.out.println("============= table: " + tableName + "  table_group: "
                                       + getTableName(tableName) + " already exist =============");
                }
            }
            tableNames.add(tableName);
        }
    }

    public static void alterTables(Connection conn, TableTemplateManager.TableType type,
                                   List<String> tableNames, boolean printSql) throws Exception {
        // create single cf table
        if (tableNames != null) {
            String tableGroup = TableTemplateManager.getTableGroupName(type, false);
            String tableName = TableTemplateManager.generateTableName(tableGroup, false, 1);
            String sql = TableTemplateManager.getAlterTableSQL(type, tableName);
            try {
                System.out.println(sql);
                conn.createStatement().execute(sql);
                System.out.println("============= alter table: " + tableName + "  table_group: "
                        + getTableName(tableName) + " =============\n"
                        + (printSql ? sql : "")
                        + " \n============= done =============\n");
            } catch (SQLSyntaxErrorException e) {
                if (!e.getMessage().contains("already exists")) {
                    throw e;
                } else {
                    System.out.println("============= table: " + tableName + "  table_group: "
                            + getTableName(tableName) + " alter failed =============");
                }
            }
        }
    }

    public static void createTables(Connection conn, TableTemplateManager.TableType type, Map<String, List<String>> group2tableNames, boolean printSql) throws Exception {
        if (group2tableNames != null) {
            TimeGenerator.TimeRange timeRange = TimeGenerator.generateTestTimeRange();
            String tableGroup = TableTemplateManager.getTableGroupName(type, true);
            String tableGroupSql = TableTemplateManager.generateTableGroupSQL(tableGroup);
            try {
                conn.createStatement().execute(tableGroupSql);
                System.out.println("============= create table_group: " + getTableName(tableGroup) + " =============\n" + (printSql ? tableGroupSql : "") + " \n============= done =============\n");
            } catch (SQLSyntaxErrorException e) {
                if (!e.getMessage().contains("already exists")) {
                    throw e;
                } else {
                    System.out.println("============= table_group: " + getTableName(tableGroup) + " already exist =============");
                }
            }
            group2tableNames.put(tableGroup, new LinkedList<>());
            for (int i = 1; i <= 3; ++i) {
                String tableName = TableTemplateManager.generateTableName(tableGroup, true, i);
                String sql = TableTemplateManager.getCreateTableSQL(type, tableName, timeRange);
                try {
                    conn.createStatement().execute(sql);
                    System.out.println("============= create table: " + tableName
                            + "  table_group: " + getTableName(tableName) + " =============\n"
                            + (printSql ? sql : "") + " \n============= done =============\n");
                } catch (SQLSyntaxErrorException e) {
                    if (!e.getMessage().contains("already exists")) {
                        throw e;
                    } else {
                        System.out.println("============= table: " + tableName + "  table_group: " + getTableName(tableName) + " already exist =============");
                    }
                }
                group2tableNames.get(tableGroup).add(tableName);
            }
        }
    }

    public static void alterTables(Connection conn, TableTemplateManager.TableType type, Map<String,
                                   List<String>> group2tableNames, boolean printSql) throws Exception {
        if (group2tableNames != null) {
            String tableGroup = TableTemplateManager.getTableGroupName(type, true);
            group2tableNames.put(tableGroup, new LinkedList<>());
            for (int i = 1; i <= 3; ++i) {
                String tableName = TableTemplateManager.generateTableName(tableGroup, true, i);
                String sql = TableTemplateManager.getAlterTableSQL(type, tableName);
                try {
                    conn.createStatement().execute(sql);
                    System.out.println("============= alter table: " + tableName
                            + "  table_group: " + getTableName(tableName) + " =============\n"
                            + (printSql ? sql : "") + " \n============= done =============\n");
                } catch (SQLSyntaxErrorException e) {
                    if (!e.getMessage().contains("already exists")) {
                        throw e;
                    } else {
                        System.out.println("============= table: " + tableName + "  table_group: " + getTableName(tableName) + " alter failed =============");
                    }
                }
            }
        }
    }

    public static void truncateTables(List<String> tableNames,
                                      Map<String, List<String>> group2tableNames) throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        // truncate single cf table
        truncateTables(conn, tableNames);
        // truncate multi cf table
        truncateTables(conn, group2tableNames);
    }

    public static void truncateTables(Connection conn, List<String> tableNames) throws Exception {
        if (tableNames != null) {
            for (int i = 0; i < tableNames.size(); i++) {
                String stmt = "TRUNCATE TABLE " + tableNames.get(i) + ";";
                conn.createStatement().execute(stmt);
                System.out.println("============= truncate table " + tableNames.get(i)
                                   + " done =============");
            }
        }
    }

    public static void truncateTables(Connection conn, Map<String, List<String>> group2tableNames)
                                                                                                  throws Exception {
        if (group2tableNames != null) {
            for (Map.Entry<String, List<String>> entry : group2tableNames.entrySet()) {
                for (String tableName : entry.getValue()) {
                    String stmt = "TRUNCATE TABLE " + tableName + ";";
                    conn.createStatement().execute(stmt);
                    System.out.println("============= truncate table " + tableName
                                       + " done =============");
                }
            }
        }
    }

    public static void dropTables(List<String> tableNames,
                                  Map<String, List<String>> group2tableNames) throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        // drop single cf table
        dropTables(conn, tableNames);
        // drop multi cf table
        dropTables(conn, group2tableNames);
    }

    public static void dropTables(Connection conn, List<String> tableNames) throws Exception {
        if (tableNames != null) {
            for (String tableName : tableNames) {
                String stmt = "DROP TABLE IF EXISTS " + tableName + ";";
                conn.createStatement().execute(stmt);
                System.out.println("============= drop table " + tableName + " done =============");
            }
        }
    }

    public static void dropTables(Connection conn, Map<String, List<String>> group2tableNames)
                                                                                              throws Exception {
        if (group2tableNames != null) {
            for (Map.Entry<String, List<String>> entry : group2tableNames.entrySet()) {
                for (String tableName : entry.getValue()) {
                    String stmt = "DROP TABLE IF EXISTS " + tableName + ";";
                    conn.createStatement().execute(stmt);
                    System.out.println("============= drop table " + tableName
                                       + " done =============");
                }
                String stmt = "DROP TABLEGROUP IF EXISTS " + entry.getKey() + ";";
                conn.createStatement().execute(stmt);
                System.out.println("============= drop tablegroup " + entry.getKey()
                                   + " done =============");
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

    public static void alterTableTimeToLive(List<String> tableNames, boolean printSql,
                                            long timeToLive) throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        if (tableNames != null) {
            for (String tableName : tableNames) {
                String alterTableTTLSQL = "ALTER TABLE "
                                          + tableName
                                          + String
                                              .format(
                                                  " kv_attributes ='{\"Hbase\": {\"TimeToLive\": %d}}';",
                                                  timeToLive);
                try {
                    conn.createStatement().execute(alterTableTTLSQL);
                    System.out.println("============= alter table ttl: " + tableName
                                       + " table_group: " + getTableName(tableName)
                                       + " =============\n" + (printSql ? alterTableTTLSQL : "")
                                       + " \n============= done =============\n");
                } catch (SQLSyntaxErrorException e) {
                    throw e;
                }
            }
        }
    }

    public static void alterTableMaxVersion(List<String> tableNames, boolean printSql,
                                            long maxVersion) throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        if (tableNames != null) {
            for (String tableName : tableNames) {
                String alterTableTTLSQL = "ALTER TABLE "
                                          + tableName
                                          + String
                                              .format(
                                                  " kv_attributes ='{\"Hbase\": {\"MaxVersions\": %d}}';",
                                                  maxVersion);
                try {
                    conn.createStatement().execute(alterTableTTLSQL);
                    System.out.println("============= alter table ttl: " + tableName
                                       + " table_group: " + getTableName(tableName)
                                       + " =============\n" + (printSql ? alterTableTTLSQL : "")
                                       + " \n============= done =============\n");
                } catch (SQLSyntaxErrorException e) {
                    throw e;
                }
            }
        }
    }

    public static int getSQLTableRowCnt(String tableName) throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        String RowCountSQL = "SELECT COUNT(*) FROM " + tableName + ";";
        ResultSet resultSet = conn.createStatement().executeQuery(RowCountSQL);
        int rowCnt = 0;
        if (resultSet.next()) {
            rowCnt = resultSet.getInt(1);
        }
        System.out.println("============= rowCnt: " + rowCnt + " =============");
        return rowCnt;
    }

    public static int getRunningNormalTTLTaskCnt() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        String RowCountSQL = "SELECT COUNT(*) FROM "
                             + "OCEANBASE.DBA_OB_KV_TTL_TASKS where TASK_TYPE = 'NORMAL'";
        ResultSet resultSet = conn.createStatement().executeQuery(RowCountSQL);
        int rowCnt = 0;
        if (resultSet.next()) {
            rowCnt = resultSet.getInt(1);
        }
        return rowCnt;
    }

    public static void enableTTL() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        String stmt = "ALTER SYSTEM set enable_kv_ttl = true;";
        conn.createStatement().execute(stmt);
    }

    public static void triggerTTL() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        String stmt = "ALTER SYSTEM trigger TTL;";
        conn.createStatement().execute(stmt);
    }

    public static void disableTTL() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        String stmt = "ALTER SYSTEM set enable_kv_ttl = false;";
        conn.createStatement().execute(stmt);
    }

    public static void AssertKeyValue(String key, String qualifier, long timestamp, String value,
                                      Cell cell) {
        Assert.assertEquals(key, Bytes.toString(cell.getRow()));
        Assert.assertEquals(qualifier, Bytes.toString(cell.getQualifier()));
        Assert.assertEquals(timestamp, cell.getTimestamp());
        Assert.assertEquals(value, Bytes.toString(cell.getValue()));
    }

    public static void AssertKeyValue(String key, String qualifier, String value, Cell cell) {
        Assert.assertEquals(key, Bytes.toString(cell.getRow()));
        Assert.assertEquals(qualifier, Bytes.toString(cell.getQualifier()));
        Assert.assertEquals(value, Bytes.toString(cell.getValue()));
    }

    public static void AssertKeyValue(String key, String family, String qualifier, long timestamp,
                                      String value, Cell cell) {
        Assert.assertEquals(key, Bytes.toString(cell.getRow()));
        Assert.assertEquals(family, Bytes.toString(cell.getFamily()));
        Assert.assertEquals(qualifier, Bytes.toString(cell.getQualifier()));
        Assert.assertEquals(timestamp, cell.getTimestamp());
        Assert.assertEquals(value, Bytes.toString(cell.getValue()));
    }

    public static List<Cell> getCellsFromScanner(ResultScanner scanner) {
        List<Cell> cells = new ArrayList<Cell>();
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                cells.add(cell);
            }
        }
        return cells;
    }

    public static void checkUtilTimeout(List<String> tableNames, Supplier<Boolean> function, long timeout, long interval) throws Exception {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeout) {
            if (function.get()) {
                return;
            }
            Thread.sleep(interval);
        }
        ObHTableTestUtil.Assert(tableNames, ()-> Assert.fail("Timeout while waiting for the function to return expected result"));
    }

    public static void sortCells(Cell[] cells) {
        if (cells == null || cells.length <= 1) { return; }

        Arrays.sort(cells, new Comparator<Cell>() {
            @Override
            public int compare(Cell c1, Cell c2) {
                if (c1 == null) return 1;
                if (c2 == null) return -1;
                int cmpRet = Bytes.compareTo(c1.getRow(), c2.getRow());
                if (cmpRet != 0) { return cmpRet; }

                cmpRet = Bytes.compareTo(c1.getFamily(), c2.getFamily());
                if (cmpRet != 0) { return cmpRet; }

                cmpRet = Bytes.compareTo(c1.getQualifier(), c2.getQualifier());
                if (cmpRet != 0) { return cmpRet; }

                cmpRet = Long.compare(c2.getTimestamp(), c1.getTimestamp());
                return cmpRet;
            }
        });
    }
}
