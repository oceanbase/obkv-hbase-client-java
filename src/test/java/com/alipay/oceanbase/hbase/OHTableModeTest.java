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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static com.alipay.oceanbase.hbase.ObHTableTestUtil.*;
import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.*;

public class OHTableModeTest extends HTableTestBase {
    public static String tableName = "test";
    public static String mysqlCompatMode = "mysql";
    public static String oracleCompatMode = "oracle";

    public static String allKVMode = "ALL";
    public static String tableKVMode = "TABLEAPI";
    public static String hbaseKVMode = "HBASE";
    public static String RedisKVMode = "REDIS";
    public static String noneKVMode = "NONE";

    public static String tpUnit = "tpUnit";
    public static String tpPool = "tpPool";
    public static String tpTenant = "tpTenant";

    public static String hbaseUnit = "hbaseUnit";
    public static String hbasePool = "hbasePool";
    public static String hbaseTenant = "hbaseTenant";

    public static String tableUnit = "tableUnit";
    public static String tablePool = "tablePool";
    public static String tableTenant = "tableTenant";

    public static String redisUnit = "redisUnit";
    public static String redisPool = "redisPool";
    public static String redisTenant = "redisTenant";

    @Before
    public void setup() throws Exception {
    }

    public static String extractUserName(String input) {
        int atSymbolIndex = input.indexOf("@");
        if (atSymbolIndex != -1) {
            return input.substring(0, atSymbolIndex);
        } else {
            return "";
        }
    }

    public static String extractClusterName(String input) {
        int hashSymbolIndex = input.lastIndexOf("#");
        if (hashSymbolIndex != -1) {
            return input.substring(hashSymbolIndex + 1);
        } else {
            return "";
        }
    }

    public void createTable(String userName, String tenantName) throws Exception {
        String user = userName + "@" + tenantName;
        String  url                = "jdbc:mysql://" + JDBC_IP + ":" + JDBC_PORT + "/ " + "test" + "?" +
                "rewriteBatchedStatements=TRUE&" +
                "allowMultiQueries=TRUE&" +
                "useLocalSessionState=TRUE&" +
                "useUnicode=TRUE&" +
                "characterEncoding=utf-8&" +
                "socketTimeout=3000000&" +
                "connectTimeout=60000";
        Connection conn = DriverManager.getConnection(url, user, PASSWORD);
        Statement statement = conn.createStatement();
        statement.execute("CREATE TABLE IF NOT EXISTS `test$family1` (" +
                "    `K` varbinary(1024) NOT NULL," +
                "    `Q` varbinary(256) NOT NULL," +
                "    `T` bigint(20) NOT NULL," +
                "    `V` varbinary(1024) DEFAULT NULL," +
                "    PRIMARY KEY (`K`, `Q`, `T`)" +
                ");");
    }

    public void createResourceUnit(String unitName) throws Exception {
        Connection conn = ObHTableTestUtil.getSysConnection();
        Statement statement = conn.createStatement();
        statement.execute("create resource unit " + unitName + " max_cpu 1, memory_size '1G';");
    }

    public void createResourcePool(String unitName, String poolName) throws Exception {
        createResourceUnit(unitName);
        Connection conn = ObHTableTestUtil.getSysConnection();
        Statement statement = conn.createStatement();
        statement.execute("create resource pool " + poolName + " unit = '" + unitName + "', unit_num = 1;");
    }

    public void createTenant(String unitName, String poolName, String tenantName, String compatMode, String kvMode) throws Exception {
        createResourcePool(unitName, poolName);
        Connection conn = ObHTableTestUtil.getSysConnection();
        Statement statement = conn.createStatement();
        statement.execute("create tenant " + tenantName + " replica_num = 1, resource_pool_list=('"+ poolName +"') " +
                "set ob_tcp_invited_nodes='%', ob_compatibility_mode='" + compatMode + "', ob_kv_mode='" + kvMode + "';");
    }

    public void dropResourceUnit(String unitName) throws Exception {
        Connection conn = ObHTableTestUtil.getSysConnection();
        Statement statement = conn.createStatement();
        statement.execute("drop resource unit if exists " + unitName + ";");
    }

    public void dropResourcePool(String poolName) throws Exception {
        Connection conn = ObHTableTestUtil.getSysConnection();
        Statement statement = conn.createStatement();
        statement.execute("drop resource pool if exists " + poolName + ";");
    }

    public void dropTenant(String tenantName) throws Exception {
        Connection conn = ObHTableTestUtil.getSysConnection();
        Statement statement = conn.createStatement();
        statement.execute("drop tenant if exists " + tenantName + " force;");
    }

    public static Configuration newConfiguration(String fullName) {
        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_FULL_USER_NAME, fullName);
        conf.set(HBASE_OCEANBASE_PASSWORD, PASSWORD);
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

    public OHTableClient createClient(String fullName) {
        return new OHTableClient(tableName, newConfiguration(fullName));
    }

    @Test
    public void testTpTenant() throws Exception {
        try {
            createTenant(tpUnit, tpPool, tpTenant, mysqlCompatMode, noneKVMode);

            String tenantName = tpTenant;
            String userName = extractUserName(FULL_USER_NAME);
            String clusterName = extractClusterName(FULL_USER_NAME);
            String fullName = userName + "@" + tenantName + "#" + clusterName;

            createTable(userName, tenantName);

            hTable = createClient(fullName);
            ((OHTableClient) hTable).init();

            Exception thrown = assertThrows(
                    Exception.class,
                    () -> {
                        String key = "putKey";
                        String family = "family1";
                        String column = "putColumn1";
                        long timestamp = System.currentTimeMillis();
                        String value = "value";
                        Put put = new Put(toBytes(key));
                        put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
                        hTable.put(put);
                    }
            );
            System.out.println(thrown.getCause().getMessage());
            assertTrue(thrown.getCause().getMessage().contains("[-4007][OB_NOT_SUPPORTED][As the ob_kv_mode variable has been set to 'NONE', your current interfaces not supported]"));
        } finally {
            dropTenant(tpTenant);
            dropResourcePool(tpPool);
            dropResourceUnit(tpUnit);
        }
    }

    @Test
    public void testHbaseTenant() throws Exception {
        try {
            createTenant(hbaseUnit, hbasePool, hbaseTenant, mysqlCompatMode, hbaseKVMode);

            String tenantName = hbaseTenant;
            String userName = extractUserName(FULL_USER_NAME);
            String clusterName = extractClusterName(FULL_USER_NAME);
            String fullName = userName + "@" + tenantName + "#" + clusterName;

            createTable(userName, tenantName);

            hTable = createClient(fullName);
            ((OHTableClient) hTable).init();

            String key = "putKey";
            String family = "family1";
            String column = "putColumn1";
            long timestamp = System.currentTimeMillis();
            String value = "value";
            Put put = new Put(toBytes(key));
            put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
            hTable.put(put);
        } finally {
            dropTenant(hbaseTenant);
            dropResourcePool(hbasePool);
            dropResourceUnit(hbaseUnit);
        }
    }

    @Test
    public void testTableTenant() throws Exception {
        try {
            String tenantName = tableTenant;
            createTenant(tableUnit, tablePool, tenantName, mysqlCompatMode, tableKVMode);

            String userName = extractUserName(FULL_USER_NAME);
            String clusterName = extractClusterName(FULL_USER_NAME);
            String fullName = userName + "@" + tenantName + "#" + clusterName;

            createTable(userName, tenantName);

            hTable = createClient(fullName);
            ((OHTableClient) hTable).init();

            Exception thrown = assertThrows(
                    Exception.class,
                    () -> {
                        String key = "putKey";
                        String family = "family1";
                        String column = "putColumn1";
                        long timestamp = System.currentTimeMillis();
                        String value = "value";
                        Put put = new Put(toBytes(key));
                        put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
                        hTable.put(put);
                    }
            );
            System.out.println(thrown.getCause().getMessage());
            assertTrue(thrown.getCause().getMessage().contains("[-4007][OB_NOT_SUPPORTED][As the ob_kv_mode variable has been set to 'TABLEAPI', your current interfaces not supported]"));
        } finally {
            dropTenant(tableTenant);
            dropResourcePool(tablePool);
            dropResourceUnit(tableUnit);
        }
    }

    @Test
    public void testRedisTenant() throws Exception {
        try {
            String tenantName = redisTenant;
            createTenant(redisUnit, redisPool, tenantName, mysqlCompatMode, RedisKVMode);

            String userName = extractUserName(FULL_USER_NAME);
            String clusterName = extractClusterName(FULL_USER_NAME);
            String fullName = userName + "@" + tenantName + "#" + clusterName;

            createTable(userName, tenantName);

            hTable = createClient(fullName);
            ((OHTableClient) hTable).init();

            Exception thrown = assertThrows(
                    Exception.class,
                    () -> {
                        String key = "putKey";
                        String family = "family1";
                        String column = "putColumn1";
                        long timestamp = System.currentTimeMillis();
                        String value = "value";
                        Put put = new Put(toBytes(key));
                        put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
                        hTable.put(put);
                    }
            );
            System.out.println(thrown.getCause().getMessage());
            assertTrue(thrown.getCause().getMessage().contains("[-4007][OB_NOT_SUPPORTED][As the ob_kv_mode variable has been set to 'REDIS', your current interfaces not supported]"));
        } finally {
            dropTenant(redisTenant);
            dropResourcePool(redisPool);
            dropResourceUnit(redisUnit);
        }
    }
}
