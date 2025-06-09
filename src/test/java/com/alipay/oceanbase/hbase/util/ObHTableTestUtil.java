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

import com.alipay.oceanbase.hbase.OHTableClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.sql.Connection;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;

public class ObHTableTestUtil {
    // please consult your dba for the following configuration.
    public static String       PARAM_URL      = "http://ocp-cfg.alibaba.net:8080/services?User_ID=alibaba&UID=test&Action=ObRootServiceInfo&ObCluster=ob124.maochongxin.mcx.11.158.97.239&database=test";
    public static String       FULL_USER_NAME = "root@mysql_tenant#ob124.maochongxin.mcx.11.158.97.239";
    public static String       PASSWORD       = "";
    public static String       SYS_USER_NAME  = "proxyro";
    public static String       SYS_PASSWORD   = "3u^0kCdpE";
    public static String       ODP_ADDR       = "";
    public static int          ODP_PORT       = 0;
    public static boolean      ODP_MODE       = false;
    public static String       DATABASE       = "test";
    public static String       JDBC_IP        = "11.158.97.239";
    public static String       JDBC_PORT      = "55605";
    public static String       JDBC_DATABASE  = "test";
    public static String       JDBC_URL       = "jdbc:mysql://" + JDBC_IP + ":" + JDBC_PORT + "/ "
                                                + JDBC_DATABASE + "?" + "useUnicode=TRUE&"
                                                + "characterEncoding=utf-8&"
                                                + "socketTimeout=3000000&" + "connectTimeout=60000";
    public static String       SYS_JDBC_URL   = "jdbc:mysql://" + JDBC_IP + ":" + JDBC_PORT + "/ "
                                                + "oceanbase?" + "useUnicode=TRUE&"
                                                + "characterEncoding=utf-8&"
                                                + "socketTimeout=3000000&" + "connectTimeout=60000";

    public static String       SQL_FORMAT     = "truncate %s";
    public static List<String> tableNameList  = new LinkedList<String>();
    public static Connection   conn;
    public static Statement    stmt;

    static {
        conn = getConnection();
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            System.out.println("sql error " + e);
        }
    }

    public static void prepareClean(List<String> tableGroupList) throws Exception {
        for (String tableGroup : tableGroupList) {
            tableNameList.addAll(getOHTableNameList(tableGroup));
        }
    }

    public static void cleanData() {
        try {
            for (String realTableName : tableNameList) {
                try {
                    if (realTableName.contains("'")) {
                        realTableName = "`" + realTableName + "`";
                    }
                    stmt.execute(String.format(SQL_FORMAT, realTableName));
                } catch (Exception e) {
                    System.out.println("clean table data error ." + realTableName + "   exception:"
                                       + e);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeConn() throws SQLException {
        stmt.close();
        conn.close();
    }

    public static Configuration newConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HBASE_OCEANBASE_FULL_USER_NAME, FULL_USER_NAME);
        conf.set(HBASE_OCEANBASE_PASSWORD, PASSWORD);
        conf.set("rpc.execute.timeout", "20000");
        conf.set("rpc.operation.timeout", "18000");
        if (ODP_MODE) {
            // ODP mode
            conf.set(HBASE_OCEANBASE_ODP_ADDR, ODP_ADDR);
            conf.setInt(HBASE_OCEANBASE_ODP_PORT, ODP_PORT);
            conf.setBoolean(HBASE_OCEANBASE_ODP_MODE, ODP_MODE);
            conf.set(HBASE_OCEANBASE_DATABASE, DATABASE);
        } else {
            // OCP mode
            conf.set(HBASE_OCEANBASE_PARAM_URL, PARAM_URL);
            conf.set(HBASE_OCEANBASE_SYS_USER_NAME, SYS_USER_NAME);
            conf.set(HBASE_OCEANBASE_SYS_PASSWORD, SYS_PASSWORD);
        }
        return conf;
    }

    public static OHTableClient newOHTableClient(String tableName) {
        return new OHTableClient(tableName, newConfiguration());
    }

    static public List<String> getOHTableNameList(String tableGroup) throws IOException {
            // 读取建表语句
            List<String> res = new LinkedList<>();
            String sql = new String(Files.readAllBytes(Paths.get(NativeHBaseUtil.SQL_PATH)));
            String[] sqlList = sql.split(";");
            for (String singleSql : sqlList) {
                String realTableName;
                if (singleSql.contains("CREATE TABLE ")) {
                    singleSql.trim();
                    String[] splits = singleSql.split(" ");
                    String tableGroupName = splits[2].substring(1, splits[2].length() - 1);
                    if (tableGroupName.contains(":")) {
                        String[] tmpStr = tableGroupName.split(":", 2);
                        tableGroupName = tmpStr[1];
                    }
                    realTableName = tableGroupName.split("\\$", 2)[0];
                    if (realTableName.equals(tableGroup)) {
                        res.add(tableGroupName);
                    }
                }
            }
            return res;
        }

    static public Connection getConnection() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String[] userNames = FULL_USER_NAME.split("#");
            Connection conn = DriverManager.getConnection(JDBC_URL, userNames[0], PASSWORD);

            return conn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static public Connection getSysConnection() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager
                .getConnection(SYS_JDBC_URL, SYS_USER_NAME, SYS_PASSWORD);

            return conn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    public interface CheckedConsumer<T> {
        void accept(T t) throws Throwable;
    }

    public static void FOR_EACH(List<String> tableNames, CheckedConsumer<String> consumer)
                                                                                          throws Throwable {
        for (String tableName : tableNames) {
            System.out.println("============================= table::{" + tableName
                               + "} =============================");
            consumer.accept(tableName);
        }
    }

    public static void FOR_EACH(Map<String, List<String>> group2Tables,
                                CheckedConsumer<Map.Entry<String, List<String>>> consumer)
                                                                                          throws Throwable {
        for (Map.Entry<String, List<String>> entry : group2Tables.entrySet()) {
            consumer.accept(entry);
        }
    }

    public static void Assert(String tableName, Runnable assertMethod) throws SQLException {
        try {
            assertMethod.run();
        } catch (AssertionError e) {
            Connection conn = ObHTableTestUtil.getConnection();
            String selectSql = "select * from " + tableName;
            System.out.println("assert fail, execute sql: " + selectSql);
            java.sql.ResultSet resultSet = conn.createStatement().executeQuery(selectSql);
            ResultSetPrinter.print(resultSet);
            throw e;
        }
    }

    public static void Assert(List<String> tableNames, Runnable assertMethod) throws SQLException {
        try {
            assertMethod.run();
        } catch (AssertionError e) {
            for (String tableName : tableNames) {
                Connection conn = ObHTableTestUtil.getConnection();
                String selectSql = "select * from " + tableName;
                System.out.println("assert fail, execute sql: " + selectSql);
                java.sql.ResultSet resultSet = conn.createStatement().executeQuery(selectSql);
                ResultSetPrinter.print(resultSet);
            }
            throw e;
        }
    }

    public static boolean secureCompare(byte[] a, byte[] b) {
        int diff = a.length ^ b.length;
        for (int i = 0; i < a.length && i < b.length; i++) {
            diff |= a[i] ^ b[i];
        }
        return diff == 0;
    }
}