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
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;

public class ObHTableTestUtil {
    // please consult your dba for the following configuration.
    public static String  PARAM_URL      = "";
    public static String  FULL_USER_NAME = "";
    public static String  PASSWORD       = "";
    public static String  SYS_USER_NAME  = "";
    public static String  SYS_PASSWORD   = "";
    public static String  ODP_ADDR       = "";
    public static int     ODP_PORT       = 0;
    public static boolean ODP_MODE       = false;
    public static String  DATABASE       = "";

    public static String  JDBC_IP                 = "";
    public static String  JDBC_PORT               = "";
    public static String  JDBC_DATABASE           = "OCEANBASE";
    public static String  JDBC_URL                = "jdbc:mysql://" + JDBC_IP + ":" + JDBC_PORT + "/ " + JDBC_DATABASE + "?" +
            "rewriteBatchedStatements=TRUE&" +
            "allowMultiQueries=TRUE&" +
            "useLocalSessionState=TRUE&" +
            "useUnicode=TRUE&" +
            "characterEncoding=utf-8&" +
            "socketTimeout=3000000&" +
            "connectTimeout=60000";

    public static Configuration newConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HBASE_OCEANBASE_FULL_USER_NAME, FULL_USER_NAME);
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

    public static OHTableClient newOHTableClient(String tableName) {
        return new OHTableClient(tableName, newConfiguration());
    }

    public static Connection getConnection() throws SQLException {
        String[] userNames = FULL_USER_NAME.split("#");
        return DriverManager.getConnection(JDBC_URL, userNames[0], PASSWORD);
    }
}