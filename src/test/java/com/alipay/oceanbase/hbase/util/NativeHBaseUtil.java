package com.alipay.oceanbase.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

public class NativeHBaseUtil {
    public static String SQL_PATH       = "src/test/java/unit_test_db.sql";
    public static String MASTER_IP_PORT = "";
    public static String ZK_QUORUM      = "";
    public static String ZK_PORT        = "";

    @Test
    public void createTable() throws IOException {
        Configuration config = new Configuration();
        if (!MASTER_IP_PORT.isEmpty()) {
            config.set("hbase.master", MASTER_IP_PORT);
        }
        config.set("hbase.zookeeper.quorum", ZK_QUORUM);
        config.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        // 建立连接
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        // 读取建表语句
        String sql = new String(Files.readAllBytes(Paths.get(SQL_PATH)));
        String[] sqlList = sql.split(";");
        Map<String, HTableDescriptor> tableMap = new LinkedHashMap<>();
        for (String singleSql : sqlList) {
            String namespace = null;
            String realTableName;
            String family;

            if (singleSql.contains("CREATE TABLE ")) {
                singleSql.trim();
                String[] splits = singleSql.split(" ");
                realTableName = splits[2].substring(1, splits[2].length() - 1);
                if (realTableName.contains(":")) {
                    String[] tmpStr = realTableName.split(":", 2);
                    namespace = tmpStr[0];
                    realTableName = tmpStr[1];
                }
                String[] tmpStr = realTableName.split("\\$", 2);
                realTableName = tmpStr[0];
                family = tmpStr[1];
                HTableDescriptor hTableDescriptor = tableMap.get(realTableName);
                if (hTableDescriptor == null) {
                    hTableDescriptor = new HTableDescriptor(TableName.valueOf(namespace, realTableName));
                    tableMap.put(realTableName, hTableDescriptor);
                }
                if (family != null) {
                    try {
                        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
                        hColumnDescriptor.setMaxVersions(1000);
                        hTableDescriptor.addFamily(hColumnDescriptor);

                    } catch (Exception e) {
                        System.out.println("family has exist");
                    }
                }
                // 不分区，结果不会有变化
            }
        }
        for (Map.Entry<String, HTableDescriptor> entry : tableMap.entrySet()) {
            if (admin.tableExists(entry.getValue().getTableName())) {
                admin.disableTable(entry.getValue().getTableName());
                admin.deleteTable(entry.getValue().getTableName());
            }
            admin.createTable(entry.getValue());
        }
    }

    static public Table getTable(TableName tableName) throws IOException {
        Configuration config = new Configuration();
        config.set("hbase.master", MASTER_IP_PORT);
        config.set("hbase.zookeeper.quorum", ZK_QUORUM);
        config.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        // 建立连接
        Connection connection = ConnectionFactory.createConnection(config);
        return connection.getTable(tableName);
    }

    static public Admin getAdmin() throws IOException {
        Configuration config = new Configuration();
        config.set("hbase.master", MASTER_IP_PORT);
        config.set("hbase.zookeeper.quorum", ZK_QUORUM);
        config.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        // 建立连接
        Connection connection = ConnectionFactory.createConnection(config);
        return connection.getAdmin();
    }

}
