/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2025 OceanBase
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.hbase.client.*;
import com.alipay.oceanbase.rpc.exception.ObTableException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;

class SqlAuditResult {
    public String svrIp;
    public int svrPort;
    public int tabletId;

    public SqlAuditResult(String svrIp, int svrPort, int tabletId) {
        this.svrIp = svrIp;
        this.svrPort = svrPort;
        this.tabletId = tabletId;
    }

    @Override
    public String toString() {
        return "SqlAuditResult{" + "svrIp='" + svrIp + '\'' + ", svrPort=" + svrPort
                + ", tabletId=" + tabletId + '}';
    }
}

class ReplicaLocation {
    public String zone;
    public String region;
    public String idc;
    public String svrIp;
    public int svrPort;
    public String role;

    public ReplicaLocation(String zone, String region, String idc, String svrIp, int svrPort,
            String role) {
        this.zone = zone;
        this.region = region;
        this.idc = idc;
        this.svrIp = svrIp;
        this.svrPort = svrPort;
        this.role = role;
    }

    public boolean isLeader() {
        return role.equalsIgnoreCase("LEADER");
    }

    public boolean isFollower() {
        return role.equalsIgnoreCase("FOLLOWER");
    }

    public String getZone() {
        return zone;
    }

    public String getRegion() {
        return region;
    }

    public String getIdc() {
        return idc;
    }

    public String getSvrIp() {
        return svrIp;
    }

    public int getSvrPort() {
        return svrPort;
    }

    public String getRole() {
        return role;
    }

    public String toString() {
        return "ReplicaLocation{" + "zone=" + zone + ", region=" + region + ", idc=" + idc
                + ", svrIp=" + svrIp + ", svrPort=" + svrPort + ", role=" + role + '}';
    }
}

class PartitionLocation {
    ReplicaLocation leader;
    List<ReplicaLocation> replicas;

    public PartitionLocation(ReplicaLocation leader, List<ReplicaLocation> replicas) {
        this.leader = leader;
        this.replicas = replicas;
    }

    public ReplicaLocation getLeader() {
        return leader;
    }

    public List<ReplicaLocation> getReplicas() {
        return replicas;
    }

    public ReplicaLocation getReplicaBySvrAddr(String svrIp, int svrPort) throws Exception {
        if (leader.svrIp.equals(svrIp) && leader.svrPort == svrPort) {
            return leader;
        }
        for (ReplicaLocation replica : replicas) {
            if (replica.svrIp.equals(svrIp) && replica.svrPort == svrPort) {
                return replica;
            }
        }
        throw new Exception("Failed to get replica from partition location for svrIp: " + svrIp
                + " and svrPort: " + svrPort);
    }

    public String toString() {
        return "PartitionLocation{" + "leader=" + leader + ", replicas=" + replicas + '}';
    }
}

/*
 * CREATE TABLE IF NOT EXISTS `test_weak_read` (
 * `c1` varchar(20) NOT NULL,
 * `c2` varchar(20) default NULL,
 * PRIMARY KEY (`c1`)
 * ) PARTITION BY KEY(`c1`) PARTITIONS 97;
 */
public class ObTableWeakReadTest {
    // 测试配置常量
    private static String FULL_USER_NAME = "";
    private static String PARAM_URL = "";
    private static String PASSWORD = "";
    private static String PROXY_SYS_USER_NAME = "root";
    private static String PROXY_SYS_USER_PASSWORD = "";
    private static boolean USE_ODP = false;
    private static String ODP_IP = "ip-addr";
    private static int ODP_PORT = 0;
    private static int ODP_SQL_PORT = 0;
    private static String ODP_DATABASE = "database-name";
    private static String JDBC_IP = "";
    private static String JDBC_PORT = "";
    private static String JDBC_DATABASE = "test";
    private static String JDBC_URL = "jdbc:mysql://"
            + JDBC_IP
            + ":"
            + JDBC_PORT
            + "/"
            + JDBC_DATABASE
            + "?rewriteBatchedStatements=TRUE&allowMultiQueries=TRUE&useLocalSessionState=TRUE&useUnicode=TRUE&characterEncoding=utf-8&socketTimeout=30000000&connectTimeout=600000&sessionVariables=ob_query_timeout=60000000000";
    private static String JDBC_PROXY_URL = "jdbc:mysql://"
            + ODP_IP
            + ":"
            + ODP_SQL_PORT
            + "/"
            + JDBC_DATABASE
            + "?rewriteBatchedStatements=TRUE&allowMultiQueries=TRUE&useLocalSessionState=TRUE&useUnicode=TRUE&characterEncoding=utf-8&socketTimeout=30000000&connectTimeout=600000&sessionVariables=ob_query_timeout=60000000000";

    private static boolean printDebug = true;
    private static int SQL_AUDIT_PERSENT = 20;
    private static String TENANT_NAME = "mysql";
    private static String TABLE_NAME = "test_weak_read";
    private static String FAMILY_NAME = "cf";
    private int tenant_id = 0;
    private static String FOLLOW_FIRST_ROUTE_POLICY = "FOLLOWER_FIRST";
    private static String FOLLOW_ONLY_ROUTE_POLICY = "FOLLOWER_ONLY";
    private static String ZONE1 = "zone1";
    private static String ZONE2 = "zone2";
    private static String ZONE3 = "zone3";
    private static String IDC1 = "idc1";
    private static String IDC2 = "idc2";
    private static String IDC3 = "idc3";
    private static String SWITCH_DISTRICT_SQL = "ALTER SYSTEM SET _obkv_enable_distributed_execution = ?;";
    private static String CREATE_TABLE_GROUP_SQL = "CREATE TABLEGROUP IF NOT EXISTS `%s` SHARDING = 'ADAPTIVE';";
    private static String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS `%s$%s` ( "
            + " K varbinary(1024), "
            + " Q varbinary(256), "
            + " T bigint, "
            + " V varbinary(1048576), "
            + " PRIMARY KEY(K, Q, T) "
            + " ) TABLEGROUP = `%s` PARTITION BY KEY(`K`) PARTITIONS 97;";
    private static String SQL_AUDIT_SQL = "select svr_ip, svr_port, query_sql from oceanbase.GV$OB_SQL_AUDIT "
            + "where query_sql like ? and query_sql like ? and tenant_id = ? limit 1;";
    private static String PARTITION_LOCATION_SQL = "SELECT t.zone, t.svr_ip, t.svr_port, t.role, z.idc, z.region "
            + "FROM oceanbase.CDB_OB_TABLE_LOCATIONS t JOIN oceanbase.DBA_OB_ZONES z ON t.zone = z.zone "
            + "WHERE t.table_name = ? AND t.tenant_id = ? AND t.tablet_id = ?;";
    private static String SET_SQL_AUDIT_PERSENT_SQL = "SET GLOBAL ob_sql_audit_percentage =?;";
    private static String GET_TENANT_ID_SQL = "SELECT tenant_id FROM oceanbase.__all_tenant WHERE tenant_name = ?";
    private static String SET_IDC_SQL = "ALTER PROXYCONFIG SET proxy_idc_name = ?;";
    private static String SET_ROUTE_POLICY_SQL = "ALTER PROXYCONFIG SET proxy_route_policy = ?;";
    private Connection tenantConnection = null;
    private Connection sysConnection = null;
    private Connection proxyConnection = null;
    private org.apache.hadoop.hbase.client.Connection connection = null;
    private static Connection staticTenantConnection = null;
    private static Connection staticSysConnection = null;
    private static Connection staticProxyConnection = null;
    private static org.apache.hadoop.hbase.client.Connection staticonnection = null;
    private static int staticTenantId = 0;
    private static boolean clear = false;

    private static void initObkvConfig(Configuration config, String idc, String routePolicy) {
        config.set(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL,
                "com.alipay.oceanbase.hbase.util.OHConnectionImpl");
        if (USE_ODP) {
            config.setBoolean(HBASE_OCEANBASE_ODP_MODE, true);
            config.set(HBASE_OCEANBASE_ODP_ADDR, ODP_IP);
            config.setInt(HBASE_OCEANBASE_ODP_PORT, ODP_PORT);
            config.set(HBASE_OCEANBASE_DATABASE, ODP_DATABASE);
            // 在 ODP 模式下，IDC 和路由策略通过 SQL 设置，不在这里设置
        } else {
            config.set(HBASE_OCEANBASE_PARAM_URL, PARAM_URL);
            config.set(HBASE_OCEANBASE_SYS_USER_NAME, PROXY_SYS_USER_NAME);
            config.set(HBASE_OCEANBASE_SYS_PASSWORD, PROXY_SYS_USER_PASSWORD);
            config.set(HBASE_OCEANBASE_FULL_USER_NAME, FULL_USER_NAME);
            config.set(HBASE_OCEANBASE_PASSWORD, PASSWORD);
            // 在非 ODP 模式下，IDC 和路由策略通过配置设置
            if (idc != null) {
                config.set(HBASE_HTABLE_CLIENT_IDC, idc);
            }
            config.set(HBASE_HTABLE_CLIENT_ROUTE_POLICY, routePolicy);
        }
    }

    /**
     * 获取租户连接
     */
    private static Connection getConnection() throws SQLException {
        String[] userNames = FULL_USER_NAME.split("#");
        return DriverManager.getConnection(JDBC_URL, userNames[0], PASSWORD);
    }

    /**
     * 获取系统连接
     */
    private static Connection getSysConnection() throws SQLException {
        return DriverManager.getConnection(JDBC_URL, "root@sys", PASSWORD);
    }

    /**
     * 获取代理连接
     */
    private static Connection getProxyConnection() throws SQLException {
        if (USE_ODP) {
            // ODP 连接需要包含集群信息的用户名
            // FULL_USER_NAME 格式: user@tenant#cluster
            // 对于代理连接，使用 root@sys#cluster 格式
            String[] parts = FULL_USER_NAME.split("#");
            String clusterName = parts.length > 1 ? parts[1] : "";
            String proxyUserName = PROXY_SYS_USER_NAME + "@sys";
            if (!clusterName.isEmpty()) {
                proxyUserName += "#" + clusterName;
            }
            return DriverManager.getConnection(JDBC_PROXY_URL, proxyUserName, PROXY_SYS_USER_PASSWORD);
        } else {
            return null;
        }
    }

    @org.junit.BeforeClass
    public static void beforeClass() throws Exception {
        // 所有测试用例执行前创建表和连接（只执行一次）
        staticTenantConnection = getConnection();
        staticSysConnection = getSysConnection();
        staticProxyConnection = getProxyConnection();
        staticTenantId = getTenantId(staticSysConnection);
        createTable(staticTenantConnection);
        setSqlAuditPersent(staticTenantConnection, SQL_AUDIT_PERSENT);
    }

    @org.junit.AfterClass
    public static void afterClass() throws Exception {
        if (clear) {
            dropTable(staticTenantConnection);
        }
    }

    @Before
    public void setup() throws Exception {
        tenantConnection = staticTenantConnection;
        sysConnection = staticSysConnection;
        proxyConnection = staticProxyConnection;
        tenant_id = staticTenantId;
        connection = staticonnection;
    }

    @After
    public void tearDown() throws Exception {
        if (clear) {
            cleanupAllData(tenantConnection);
        }
    }

    /*
     * 切换分布式执行开关
     * 
     * @param connection 数据库连接
     * 
     * @param enable 是否开启分布式执行
     */
    private static void switchDistributedExecution(Connection connection, boolean enable)
            throws Exception {
        PreparedStatement statement = connection.prepareStatement(SWITCH_DISTRICT_SQL);
        statement.setBoolean(1, enable);
        statement.execute();
        statement.close();
    }

    /**
     * 使用SQL清理所有测试数据
     * 
     * @param connection 数据库连接
     */
    private static void cleanupAllData(Connection connection) throws Exception {
        try {
            PreparedStatement statement = connection.prepareStatement("DELETE FROM " + TABLE_NAME
                    + "$" + FAMILY_NAME);
            int deletedRows = statement.executeUpdate();
            if (printDebug) {
                System.out.println("[DEBUG] Cleaned up " + deletedRows + " rows from table "
                        + TABLE_NAME + "$" + FAMILY_NAME);
            }
            statement.close();
        } catch (Exception e) {
            if (printDebug) {
                System.out.println("[DEBUG] Failed to cleanup data from table " + TABLE_NAME + "$"
                        + FAMILY_NAME + ", error: " + e.getMessage());
            }
            // 清理失败不影响测试，只打印警告
        }
    }

    private static void dropTable(Connection connection) throws Exception {
        String sql = String.format("DROP TABLE IF EXISTS `%s$%s`", TABLE_NAME, FAMILY_NAME);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.execute();
        statement.close();
        dropTableGroup(connection);
    }

    private static void dropTableGroup(Connection connection) throws Exception {
        PreparedStatement statement = connection.prepareStatement("DROP TABLEGROUP IF EXISTS "
                + TABLE_NAME);
        statement.execute();
        statement.close();
    }

    private static void createTableGroup(Connection connection) throws Exception {
        String sql = String.format(CREATE_TABLE_GROUP_SQL, TABLE_NAME);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.execute();
        statement.close();
    }

    private static void createTable(Connection connection) throws Exception {
        createTableGroup(connection);
        String sql = String.format(CREATE_TABLE_SQL, TABLE_NAME, FAMILY_NAME, TABLE_NAME);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.execute();
        statement.close();
    }

    private void setZoneIdc(String zone, String idc) throws Exception {
        PreparedStatement statement = sysConnection
                .prepareStatement("ALTER SYSTEM MODIFY ZONE ? SET IDC = ?;");
        statement.setString(1, zone);
        statement.setString(2, idc);
        debugPrint("setZoneIdc SQL: %s", statement.toString());
        statement.execute();
    }

    // 通过当前纳秒时间戳生成随机字符串
    private String getRandomRowkString() {
        return System.nanoTime() + "";
    }

    // 从 querySql 中提取 tablet_id
    private int extractTabletId(String querySql) {
        // 查找 tablet_id:{id: 的模式
        String pattern = "tablet_id:{id:";
        int startIndex = querySql.indexOf(pattern);
        if (startIndex == -1) {
            return -1;
        }
        // 找到 id: 后面的数字开始位置
        int idStartIndex = startIndex + pattern.length();
        // 跳过可能的空格
        while (idStartIndex < querySql.length()
                && Character.isWhitespace(querySql.charAt(idStartIndex))) {
            idStartIndex++;
        }
        // 找到数字结束位置（遇到 } 或 , 或空格）
        int idEndIndex = idStartIndex;
        while (idEndIndex < querySql.length()) {
            char c = querySql.charAt(idEndIndex);
            if (c == '}' || c == ',' || c == ' ') {
                break;
            }
            idEndIndex++;
        }
        // 提取数字字符串
        String tabletIdStr = querySql.substring(idStartIndex, idEndIndex).trim();
        try {
            return Integer.parseInt(tabletIdStr);
        } catch (NumberFormatException e) {
            debugPrint("Failed to parse tablet_id from: %s", tabletIdStr);
            return -1;
        }
    }

    private static int getTenantId(Connection connection) throws Exception {
        PreparedStatement statement = connection.prepareStatement(GET_TENANT_ID_SQL);
        statement.setString(1, TENANT_NAME);
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            return resultSet.getInt("tenant_id");
        }
        throw new ObTableException("Failed to get tenant id for tenant: " + TENANT_NAME);
    }

    private static void setSqlAuditPersent(Connection connection, int persent) throws Exception {
        PreparedStatement statement = connection.prepareStatement(SET_SQL_AUDIT_PERSENT_SQL);
        statement.setInt(1, persent);
        statement.execute();
    }

    // 通过SQL审计获取服务器地址,确认rowkey落在哪个服务器上
    private SqlAuditResult getServerBySqlAudit(String rowkey, String QuerySqlLikeString)
            throws Exception {
        SqlAuditResult sqlAuditResult = null;
        PreparedStatement statement = tenantConnection.prepareStatement(SQL_AUDIT_SQL);
        statement.setString(1, "%" + rowkey + "%");
        statement.setString(2, "%" + QuerySqlLikeString + "%");
        statement.setInt(3, this.tenant_id);
        debugPrint("SQL: %s", statement.toString());
        try {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String svrIp = resultSet.getString("svr_ip");
                int svrPort = resultSet.getInt("svr_port");
                String querySql = resultSet.getString("query_sql");
                int tabletId = extractTabletId(querySql);
                sqlAuditResult = new SqlAuditResult(svrIp, svrPort, tabletId);
                debugPrint("querySql: %s", querySql);
                debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
            }
            resultSet.close();
        } finally {
            statement.close();
        }

        if (sqlAuditResult == null) {
            throw new ObTableException("Failed to get server address from sql audit for rowkey: "
                    + rowkey + " and QuerySqlLikeString: " + QuerySqlLikeString);
        }

        return sqlAuditResult;
    }

    private PartitionLocation getPartitionLocation(int tabletId) throws Exception {
        ReplicaLocation leader = null;
        List<ReplicaLocation> replicas = new ArrayList<>();
        PreparedStatement statement = sysConnection.prepareStatement(PARTITION_LOCATION_SQL);
        statement.setString(1, TABLE_NAME + "$" + FAMILY_NAME);
        statement.setInt(2, this.tenant_id); // 使用成员变量 tenant_id
        statement.setInt(3, tabletId);
        debugPrint("PARTITION_LOCATION_SQL: %s", statement.toString());
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            String zone = resultSet.getString("zone");
            String region = resultSet.getString("region");
            String idc = resultSet.getString("idc");
            String svrIp = resultSet.getString("svr_ip");
            int svrPort = resultSet.getInt("svr_port");
            String role = resultSet.getString("role");
            if (role.equalsIgnoreCase("LEADER")) {
                leader = new ReplicaLocation(zone, region, idc, svrIp, svrPort, role);
            } else {
                replicas.add(new ReplicaLocation(zone, region, idc, svrIp, svrPort, role));
            }
        }
        if (leader == null) {
            throw new ObTableException("Failed to get leader from partition location for tabletId: " + tabletId);
        }
        return new PartitionLocation(leader, replicas);
    }

    private void insertData(Table table, String rowkey) throws Exception {
        Put put = new Put(rowkey.getBytes());
        put.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes(), "c2_val".getBytes());
        table.put(put);
    }

    /**
     * 封装debug打印方法
     * 
     * @param message 要打印的消息
     */
    private void debugPrint(String message) {
        if (printDebug) {
            System.out.println("[DEBUG] " + message);
        }
    }

    /**
     * 封装debug打印方法（支持格式化）
     * 
     * @param format 格式化字符串
     * @param args   参数
     */
    private void debugPrint(String format, Object... args) {
        if (printDebug) {
            System.out.println("[DEBUG] " + String.format(format, args));
        }
    }

    private void setIdc(Configuration config, String idc) throws Exception {
        if (USE_ODP) {
            if (proxyConnection != null) {
                PreparedStatement statement = proxyConnection.prepareStatement(SET_IDC_SQL);
                statement.setString(1, idc);
                statement.execute();
                statement.close();
            }
        } else {
            if (idc != null) {
                config.set(HBASE_HTABLE_CLIENT_IDC, idc);
            }
        }
    }

    private void setRoutePolicy(Configuration config, String routePolicy) throws Exception {
        if (USE_ODP) {
            if (proxyConnection != null) {
                PreparedStatement statement = proxyConnection.prepareStatement(SET_ROUTE_POLICY_SQL);
                statement.setString(1, routePolicy);
                statement.execute();
                statement.close();
            }
        } else {
            config.set(HBASE_HTABLE_CLIENT_ROUTE_POLICY, routePolicy);
        }
    }

    /*
     * 测试场景：用户正常使用场景，使用get接口进行指定IDC读
     * 测试预期：发到对应的IDC上进行读取
     */
    @Test
    public void testIdcGet1() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        // 在 ODP 模式下，需要在创建连接前通过 SQL 设置 IDC 和路由策略
        if (USE_ODP) {
            setIdc(config, IDC2);
            setRoutePolicy(config, FOLLOW_FIRST_ROUTE_POLICY);
        }
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));

        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertEquals(IDC2, readReplica.getIdc());
    }

    /*
     * 测试场景：未设置当前IDC进行弱读
     * 测试预期：发到任意follower上进行弱读
     */
    @Test
    public void testIdcGet2() throws Exception {
        Configuration config = HBaseConfiguration.create();
        // 不设置 IDC
        initObkvConfig(config, null, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：指定了IDC，但是没有指定弱读
     * 测试预期：发到leader副本上进行读取
     */
    @Test
    public void testIdcGet3() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据，不设置弱读
        Get get = new Get(rowkey.getBytes());
        // 不设置 weak consistency
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：设置不存在的IDC进行弱读
     * 测试预期：fallback到其他可用的副本（sameRegion或otherRegion）
     */
    @Test
    public void testIdcGet4() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, "invalid_idc", FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：应该读到follower，且IDC不是invalid_idc
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
        Assert.assertNotEquals("invalid_idc", readReplica.getIdc());
    }

    /*
     * 测试场景：设置IDC并使用strong consistency
     * 测试预期：即使设置了IDC，strong consistency也应该读leader
     */
    @Test
    public void testIdcGet5() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据，使用strong consistency
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "strong".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：strong consistency应该读leader，即使设置了IDC
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：设置空字符串IDC进行弱读
     * 测试预期：fallback到其他可用的副本
     */
    @Test
    public void testIdcGet6() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, "", FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：应该读到follower
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：使用非法的ReadConsistency值
     * 测试预期：可能不会抛出异常，但应该被忽略或使用默认值
     * 注意：HBase API 中，非法的 consistency 值可能不会立即抛出异常，而是在服务端处理
     */
    @Test
    public void testIdcGet7() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000);
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 使用非法的ReadConsistency值
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "invalid_consistency".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        // 执行查询，可能不会抛出异常，但应该使用默认的 strong consistency
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 4. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：非法的 consistency 值应该被忽略，使用默认的 strong，读 leader
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        // 非法的值应该被忽略，使用默认的 strong consistency
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：设置IDC但该IDC没有该分区的副本（极端情况）
     * 测试预期：fallback到其他region的副本
     */
    @Test
    public void testIdcGet8() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, "idc4", FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000);
        // 2. 设置 idc（只设置IDC1, IDC2, IDC3，不设置IDC4）
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 4. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：应该读到follower，且IDC不是idc4
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
        Assert.assertNotEquals("idc4", readReplica.getIdc());
    }

    /*
     * 测试场景：使用null作为ReadConsistency（使用默认值）
     * 测试预期：使用默认的strong consistency，读leader
     */
    @Test
    public void testIdcGet9() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000);
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 不设置ReadConsistency，使用默认值（应该是strong）
        Get get = new Get(rowkey.getBytes());
        // 不设置 weak consistency，使用默认值
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 4. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：默认应该是strong，读leader
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：设置IDC，但使用大小写不同的weak值
     * 测试预期：应该能正常识别（不区分大小写）
     */
    @Test
    public void testIdcGet10() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000);
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 使用不同大小写的weak
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "WEAK".getBytes()); // 大写
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 4. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：应该读到follower
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：关闭分布式开关，用户正常使用场景，使用get接口进行指定IDC读
     * 测试预期：发到对应的IDC上进行读取
     */
    @Test
    public void testIdcGet11() throws Exception {
        try {
            // 1. 关闭分布式开关
            switchDistributedExecution(tenantConnection, false);
            Configuration config = HBaseConfiguration.create();
            initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
            org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                    .createConnection(config);
            Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
            // 2. 准备数据
            String rowkey = getRandomRowkString();
            insertData(table, rowkey);
            Thread.sleep(1000); // 等待数据同步到所有节点
            // 3. 设置 idc
            setZoneIdc(ZONE1, IDC1);
            setZoneIdc(ZONE2, IDC2);
            setZoneIdc(ZONE3, IDC3);
            // 4. 获取数据
            Get get = new Get(rowkey.getBytes());
            get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
            get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
            Result result = table.get(get);
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
            // 5. 查询 sql audit
            SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
            debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
            // 6. 查询分区的位置信息
            PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
            debugPrint("partitionLocation: %s", partitionLocation.toString());
            // 7. 校验
            ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(
                    sqlAuditResult.svrIp, sqlAuditResult.svrPort);
            debugPrint("readReplica: %s", readReplica.toString());
            Assert.assertTrue(readReplica.isFollower());
            Assert.assertEquals(IDC2, readReplica.getIdc());
        } finally {
            // 8. 恢复分布式开关
            switchDistributedExecution(tenantConnection, true);
        }
    }

    /*
     * 测试场景：不改代码的用户，在配置项中设置全局弱一致性读
     * 测试预期：发到对应的IDC上进行读取
     */
    @Test
    public void testIdcGet12() throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set(HBASE_HTABLE_READ_CONSISTENCY, "weak"); // 设置全局弱一致性读
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        Get get = new Get(rowkey.getBytes());
        // get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes()); //
        // 不设置弱一致性读
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));

        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertEquals(IDC2, readReplica.getIdc());
    }

    /*
     * 测试场景：路由策略测试，使用FOLLOW_ONLY_ROUTE_POLICY
     * 测试预期：在存在follower场景下，能够路由到follower，且能够读到数据，但读follower副本
     */
    @Test
    public void testRoutePolicyGet1() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, null/* IDC */, FOLLOW_ONLY_ROUTE_POLICY); // 不设置IDC，使用FOLLOW_ONLY_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：路由策略测试，使用FOLLOW_ONLY_ROUTE_POLICY + 设置IDC + weak read
     * 测试预期：路由到指定IDC的follower副本
     */
    @Test
    public void testRoutePolicyGet2() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_ONLY_ROUTE_POLICY); // 设置IDC，使用FOLLOW_ONLY_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
        Assert.assertEquals(IDC2, readReplica.getIdc());
    }

    /*
     * 测试场景：路由策略测试，使用FOLLOW_ONLY_ROUTE_POLICY + strong read
     * 测试预期：即使使用FOLLOW_ONLY_ROUTE_POLICY，strong read也应该读leader
     */
    @Test
    public void testRoutePolicyGet3() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, null/* IDC */, FOLLOW_ONLY_ROUTE_POLICY); // 不设置IDC，使用FOLLOW_ONLY_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据，使用strong consistency
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "strong".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验：strong read应该读leader，即使使用FOLLOW_ONLY_ROUTE_POLICY
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：路由策略测试，使用FOLLOW_ONLY_ROUTE_POLICY + 设置IDC + strong read
     * 测试预期：即使使用FOLLOW_ONLY_ROUTE_POLICY和设置IDC，strong read也应该读leader
     */
    @Test
    public void testRoutePolicyGet4() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_ONLY_ROUTE_POLICY); // 设置IDC，使用FOLLOW_ONLY_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据，使用strong consistency
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "strong".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验：strong read应该读leader，即使使用FOLLOW_ONLY_ROUTE_POLICY和设置IDC
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：路由策略对比测试，使用FOLLOW_FIRST_ROUTE_POLICY + 不设置IDC + weak read
     * 测试预期：能够路由到follower，与FOLLOW_ONLY_ROUTE_POLICY行为类似
     */
    @Test
    public void testRoutePolicyGet5() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, null/* IDC */, FOLLOW_FIRST_ROUTE_POLICY); // 不设置IDC，使用FOLLOW_FIRST_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        Result result = table.get(get);
        Assert.assertNotNull(result);
        Assert.assertNotEquals(true, result.isEmpty());
        byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
        Assert.assertEquals("c2_val", Bytes.toString(value));
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验：应该读到follower
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：用户正常使用场景，使用scan接口进行指定IDC读
     * 测试预期：发到对应的IDC上进行读取
     */
    @Test
    public void testIdcScan1() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }

        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertEquals(IDC2, readReplica.getIdc());
    }

    /*
     * 测试场景：未设置当前IDC进行弱读
     * 测试预期：发到任意follower上进行弱读
     */
    @Test
    public void testIdcScan2() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, null, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：指定了IDC，但是没有指定弱读
     * 测试预期：发到leader副本上进行读取
     */
    @Test
    public void testIdcScan3() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据，不设置弱读
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        // 不设置 weak consistency
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：设置不存在的IDC进行弱读
     * 测试预期：fallback到其他可用的副本（sameRegion或otherRegion）
     */
    @Test
    public void testIdcScan4() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, "invalid_idc", FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：应该读到follower，且IDC不是invalid_idc
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
        Assert.assertNotEquals("invalid_idc", readReplica.getIdc());
    }

    /*
     * 测试场景：设置IDC并使用strong consistency
     * 测试预期：即使设置了IDC，strong consistency也应该读leader
     */
    @Test
    public void testIdcScan5() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据，使用strong consistency
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "strong".getBytes());
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：strong consistency应该读leader，即使设置了IDC
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：设置空字符串IDC进行弱读
     * 测试预期：fallback到其他可用的副本
     */
    @Test
    public void testIdcScan6() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, "", FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：应该读到follower
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：使用非法的ReadConsistency值
     * 测试预期：可能不会抛出异常，但应该被忽略或使用默认值
     * 注意：HBase API 中，非法的 consistency 值可能不会立即抛出异常，而是在服务端处理
     */
    @Test
    public void testIdcScan7() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000);
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 使用非法的ReadConsistency值
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "invalid_consistency".getBytes());
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        // 执行查询，可能不会抛出异常，但应该使用默认的 strong consistency
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 4. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：非法的 consistency 值应该被忽略，使用默认的 strong，读 leader
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        // 非法的值应该被忽略，使用默认的 strong consistency
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：设置IDC但该IDC没有该分区的副本（极端情况）
     * 测试预期：fallback到其他region的副本
     */
    @Test
    public void testIdcScan8() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, "idc4", FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000);
        // 2. 设置 idc（只设置IDC1, IDC2, IDC3，不设置IDC4）
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 4. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：应该读到follower，且IDC不是idc4
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
        Assert.assertNotEquals("idc4", readReplica.getIdc());
    }

    /*
     * 测试场景：使用null作为ReadConsistency（使用默认值）
     * 测试预期：使用默认的strong consistency，读leader
     */
    @Test
    public void testIdcScan9() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000);
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 不设置ReadConsistency，使用默认值（应该是strong）
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        // 不设置 weak consistency，使用默认值
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 4. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：默认应该是strong，读leader
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：设置IDC，但使用大小写不同的weak值
     * 测试预期：应该能正常识别（不区分大小写）
     */
    @Test
    public void testIdcScan10() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000);
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 使用不同大小写的weak
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "WEAK".getBytes()); // 大写
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 4. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：应该读到follower
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：关闭分布式开关，用户正常使用场景，使用scan接口进行指定IDC读
     * 测试预期：发到对应的IDC上进行读取
     */
    @Test
    public void testIdcScan11() throws Exception {
        try {
            // 1. 关闭分布式开关
            switchDistributedExecution(tenantConnection, false);
            Configuration config = HBaseConfiguration.create();
            initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
            org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                    .createConnection(config);
            Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
            // 2. 准备数据
            String rowkey = getRandomRowkString();
            insertData(table, rowkey);
            Thread.sleep(1000); // 等待数据同步到所有节点
            // 3. 设置 idc
            setZoneIdc(ZONE1, IDC1);
            setZoneIdc(ZONE2, IDC2);
            setZoneIdc(ZONE3, IDC3);
            // 4. 获取数据
            Scan scan = new Scan();
            scan.setStartRow(rowkey.getBytes());
            scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
            scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
            ResultScanner resultScanner = table.getScanner(scan);
            Result result;
            while ((result = resultScanner.next()) != null) {
                Assert.assertNotNull(result);
                Assert.assertNotEquals(true, result.isEmpty());
                byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
                Assert.assertEquals("c2_val", Bytes.toString(value));
            }
            // 5. 查询 sql audit
            SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
            debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
            // 6. 查询分区的位置信息
            PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
            debugPrint("partitionLocation: %s", partitionLocation.toString());
            // 7. 校验
            ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(
                    sqlAuditResult.svrIp, sqlAuditResult.svrPort);
            debugPrint("readReplica: %s", readReplica.toString());
            Assert.assertTrue(readReplica.isFollower());
            Assert.assertEquals(IDC2, readReplica.getIdc());
        } finally {
            // 8. 恢复分布式开关
            switchDistributedExecution(tenantConnection, true);
        }
    }

    /*
     * 测试场景：不改代码的用户，在配置项中设置全局弱一致性读
     * 测试预期：发到对应的IDC上进行读取
     */
    @Test
    public void testIdcScan12() throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set(HBASE_HTABLE_READ_CONSISTENCY, "weak"); // 设置全局弱一致性读
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        // scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes()); //
        // 不设置弱一致性读
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }

        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertEquals(IDC2, readReplica.getIdc());
    }

    /*
     * 测试场景：路由策略测试，使用FOLLOW_ONLY_ROUTE_POLICY
     * 测试预期：在存在follower场景下，能够路由到follower，且能够读到数据，但读follower副本
     */
    @Test
    public void testRoutePolicyScan1() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, null/* IDC */, FOLLOW_ONLY_ROUTE_POLICY); // 不设置IDC，使用FOLLOW_ONLY_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：路由策略测试，使用FOLLOW_ONLY_ROUTE_POLICY + 设置IDC + weak read
     * 测试预期：路由到指定IDC的follower副本
     */
    @Test
    public void testRoutePolicyScan2() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_ONLY_ROUTE_POLICY); // 设置IDC，使用FOLLOW_ONLY_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
        Assert.assertEquals(IDC2, readReplica.getIdc());
    }

    /*
     * 测试场景：路由策略测试，使用FOLLOW_ONLY_ROUTE_POLICY + strong read
     * 测试预期：即使使用FOLLOW_ONLY_ROUTE_POLICY，strong read也应该读leader
     */
    @Test
    public void testRoutePolicyScan3() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, null/* IDC */, FOLLOW_ONLY_ROUTE_POLICY); // 不设置IDC，使用FOLLOW_ONLY_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据，使用strong consistency
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "strong".getBytes());
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验：strong read应该读leader，即使使用FOLLOW_ONLY_ROUTE_POLICY
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：路由策略测试，使用FOLLOW_ONLY_ROUTE_POLICY + 设置IDC + strong read
     * 测试预期：即使使用FOLLOW_ONLY_ROUTE_POLICY和设置IDC，strong read也应该读leader
     */
    @Test
    public void testRoutePolicyScan4() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_ONLY_ROUTE_POLICY); // 设置IDC，使用FOLLOW_ONLY_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据，使用strong consistency
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "strong".getBytes());
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验：strong read应该读leader，即使使用FOLLOW_ONLY_ROUTE_POLICY和设置IDC
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：路由策略对比测试，使用FOLLOW_FIRST_ROUTE_POLICY + 不设置IDC + weak read
     * 测试预期：能够路由到follower，与FOLLOW_ONLY_ROUTE_POLICY行为类似
     */
    @Test
    public void testRoutePolicyScan5() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, null/* IDC */, FOLLOW_FIRST_ROUTE_POLICY); // 不设置IDC，使用FOLLOW_FIRST_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory
                .createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据
        Scan scan = new Scan();
        scan.setStartRow(rowkey.getBytes());
        scan.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        scan.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        while ((result = resultScanner.next()) != null) {
            Assert.assertNotNull(result);
            Assert.assertNotEquals(true, result.isEmpty());
            byte[] value = result.getValue(FAMILY_NAME.getBytes(), "c2".getBytes());
            Assert.assertEquals("c2_val", Bytes.toString(value));
        }
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验：应该读到follower
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：用户正常使用场景，使用batch get接口进行指定IDC读
     * 测试预期：发到对应的IDC上进行读取
     */
    @Test
    public void testIdcBatchGet1() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(2, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        Assert.assertEquals("c2_val", Bytes.toString(res[1].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));

        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertEquals(IDC2, readReplica.getIdc());
    }

    /*
     * 测试场景：未设置当前IDC进行弱读
     * 测试预期：发到任意follower上进行弱读
     */
    @Test
    public void testIdcBatchGet2() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, null, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：指定了IDC，但是没有指定弱读
     * 测试预期：发到leader副本上进行读取
     */
    @Test
    public void testIdcBatchGet3() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据，不设置弱读
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        // 不设置 weak consistency
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：设置不存在的IDC进行弱读
     * 测试预期：fallback到其他可用的副本（sameRegion或otherRegion）
     */
    @Test
    public void testIdcBatchGet4() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, "invalid_idc", FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：应该读到follower，且IDC不是invalid_idc
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
        Assert.assertNotEquals("invalid_idc", readReplica.getIdc());
    }

    /*
     * 测试场景：设置IDC并使用strong consistency
     * 测试预期：即使设置了IDC，strong consistency也应该读leader
     */
    @Test
    public void testIdcBatchGet5() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据，使用strong consistency
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "strong".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：strong consistency应该读leader，即使设置了IDC
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：设置空字符串IDC进行弱读
     * 测试预期：fallback到其他可用的副本
     */
    @Test
    public void testIdcBatchGet6() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, "", FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：应该读到follower
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：使用非法的ReadConsistency值
     * 测试预期：可能不会抛出异常，但应该被忽略或使用默认值
     * 注意：HBase API 中，非法的 consistency 值可能不会立即抛出异常，而是在服务端处理
     */
    @Test
    public void testIdcBatchGet7() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000);
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 使用非法的ReadConsistency值
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "invalid_consistency".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        // 执行查询，可能不会抛出异常，但应该使用默认的 strong consistency
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 4. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：非法的 consistency 值应该被忽略，使用默认的 strong，读 leader
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        // 非法的值应该被忽略，使用默认的 strong consistency
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：设置IDC但该IDC没有该分区的副本（极端情况）
     * 测试预期：fallback到其他region的副本
     */
    @Test
    public void testIdcBatchGet8() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, "idc4", FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000);
        // 2. 设置 idc（只设置IDC1, IDC2, IDC3，不设置IDC4）
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 4. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：应该读到follower，且IDC不是idc4
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
        Assert.assertNotEquals("idc4", readReplica.getIdc());
    }

    /*
     * 测试场景：使用null作为ReadConsistency（使用默认值）
     * 测试预期：使用默认的strong consistency，读leader
     */
    @Test
    public void testIdcBatchGet9() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000);
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 不设置ReadConsistency，使用默认值（应该是strong）
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        // 不设置 weak consistency，使用默认值
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 4. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：默认应该是strong，读leader
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：设置IDC，但使用大小写不同的weak值
     * 测试预期：应该能正常识别（不区分大小写）
     */
    @Test
    public void testIdcBatchGet10() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000);
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 使用不同大小写的weak
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "WEAK".getBytes()); // 大写
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 4. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验：应该读到follower
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：关闭分布式开关，用户正常使用场景，使用batch get接口进行指定IDC读
     * 测试预期：发到对应的IDC上进行读取
     */
    @Test
    public void testIdcBatchGet11() throws Exception {
        try {
            // 1. 关闭分布式开关
            switchDistributedExecution(tenantConnection, false);
            Configuration config = HBaseConfiguration.create();
            initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
            org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
            Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
            // 2. 准备数据
            String rowkey = getRandomRowkString();
            insertData(table, rowkey);
            Thread.sleep(1000); // 等待数据同步到所有节点
            // 3. 设置 idc
            setZoneIdc(ZONE1, IDC1);
            setZoneIdc(ZONE2, IDC2);
            setZoneIdc(ZONE3, IDC3);
            // 4. 获取数据
            List<Get> gets = new ArrayList<>();
            Get get = new Get(rowkey.getBytes());
            get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
            get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
            gets.add(get);
            gets.add(get);
            Result[] res = table.get(gets);
            Assert.assertNotNull(res);
            Assert.assertEquals(2, res.length);
            Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
            Assert.assertEquals("c2_val", Bytes.toString(res[1].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
            // 5. 查询 sql audit
            SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
            debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
            // 6. 查询分区的位置信息
            PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
            debugPrint("partitionLocation: %s", partitionLocation.toString());
            // 7. 校验
            ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                    sqlAuditResult.svrPort);
            debugPrint("readReplica: %s", readReplica.toString());
            Assert.assertTrue(readReplica.isFollower());
            Assert.assertEquals(IDC2, readReplica.getIdc());
        } finally {
            // 8. 恢复分布式开关
            switchDistributedExecution(tenantConnection, true);
        }
    }

    /*
     * 测试场景：不改代码的用户，在配置项中设置全局弱一致性读
     * 测试预期：发到对应的IDC上进行读取
     */
    @Test
    public void testIdcBatchGet12() throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set(HBASE_HTABLE_READ_CONSISTENCY, "weak"); // 设置全局弱一致性读
        initObkvConfig(config, IDC2, FOLLOW_FIRST_ROUTE_POLICY);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 1. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 2. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 3. 获取数据
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        // get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes()); //
        // 不设置弱一致性读
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));

        // 4. 查询 sql audit，确定读请求发到哪个节点和分区上
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 5. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 6. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertEquals(IDC2, readReplica.getIdc());
    }

    /*
     * 测试场景：路由策略测试，使用FOLLOW_ONLY_ROUTE_POLICY
     * 测试预期：在存在follower场景下，能够路由到follower，且能够读到数据，但读follower副本
     */
    @Test
    public void testRoutePolicyBatchGet1() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, null/* IDC */, FOLLOW_ONLY_ROUTE_POLICY); // 不设置IDC，使用FOLLOW_ONLY_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }

    /*
     * 测试场景：路由策略测试，使用FOLLOW_ONLY_ROUTE_POLICY + 设置IDC + weak read
     * 测试预期：路由到指定IDC的follower副本
     */
    @Test
    public void testRoutePolicyBatchGet2() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_ONLY_ROUTE_POLICY); // 设置IDC，使用FOLLOW_ONLY_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
        Assert.assertEquals(IDC2, readReplica.getIdc());
    }

    /*
     * 测试场景：路由策略测试，使用FOLLOW_ONLY_ROUTE_POLICY + strong read
     * 测试预期：即使使用FOLLOW_ONLY_ROUTE_POLICY，strong read也应该读leader
     */
    @Test
    public void testRoutePolicyBatchGet3() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, null/* IDC */, FOLLOW_ONLY_ROUTE_POLICY); // 不设置IDC，使用FOLLOW_ONLY_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据，使用strong consistency
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "strong".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验：strong read应该读leader，即使使用FOLLOW_ONLY_ROUTE_POLICY
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：路由策略测试，使用FOLLOW_ONLY_ROUTE_POLICY + 设置IDC + strong read
     * 测试预期：即使使用FOLLOW_ONLY_ROUTE_POLICY和设置IDC，strong read也应该读leader
     */
    @Test
    public void testRoutePolicyBatchGet4() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, IDC2, FOLLOW_ONLY_ROUTE_POLICY); // 设置IDC，使用FOLLOW_ONLY_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据，使用strong consistency
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "strong".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验：strong read应该读leader，即使使用FOLLOW_ONLY_ROUTE_POLICY和设置IDC
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isLeader());
    }

    /*
     * 测试场景：路由策略对比测试，使用FOLLOW_FIRST_ROUTE_POLICY + 不设置IDC + weak read
     * 测试预期：能够路由到follower，与FOLLOW_ONLY_ROUTE_POLICY行为类似
     */
    @Test
    public void testRoutePolicyBatchGet5() throws Exception {
        Configuration config = HBaseConfiguration.create();
        initObkvConfig(config, null/* IDC */, FOLLOW_FIRST_ROUTE_POLICY); // 不设置IDC，使用FOLLOW_FIRST_ROUTE_POLICY
        org.apache.hadoop.hbase.client.Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 准备数据
        String rowkey = getRandomRowkString();
        insertData(table, rowkey);
        Thread.sleep(1000); // 等待数据同步到所有节点
        // 3. 设置 idc
        setZoneIdc(ZONE1, IDC1);
        setZoneIdc(ZONE2, IDC2);
        setZoneIdc(ZONE3, IDC3);
        // 4. 获取数据
        List<Get> gets = new ArrayList<>();
        Get get = new Get(rowkey.getBytes());
        get.setAttribute(HBASE_HTABLE_READ_CONSISTENCY, "weak".getBytes());
        get.addColumn(FAMILY_NAME.getBytes(), "c2".getBytes());
        gets.add(get);
        Result[] res = table.get(gets);
        Assert.assertNotNull(res);
        Assert.assertEquals(1, res.length);
        Assert.assertEquals("c2_val", Bytes.toString(res[0].getValue(FAMILY_NAME.getBytes(), "c2".getBytes())));
        // 5. 查询 sql audit
        SqlAuditResult sqlAuditResult = getServerBySqlAudit(rowkey, "scan");
        debugPrint("sqlAuditResult: %s", sqlAuditResult.toString());
        // 6. 查询分区的位置信息
        PartitionLocation partitionLocation = getPartitionLocation(sqlAuditResult.tabletId);
        debugPrint("partitionLocation: %s", partitionLocation.toString());
        // 7. 校验：应该读到follower
        ReplicaLocation readReplica = partitionLocation.getReplicaBySvrAddr(sqlAuditResult.svrIp,
                sqlAuditResult.svrPort);
        debugPrint("readReplica: %s", readReplica.toString());
        Assert.assertTrue(readReplica.isFollower());
    }
}
