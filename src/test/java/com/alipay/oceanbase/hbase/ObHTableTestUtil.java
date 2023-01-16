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

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;

public class ObHTableTestUtil {
    // please consult your dba for the following configuration.
    public static String PARAM_URL      = "";
    public static String FULL_USER_NAME = "";
    public static String PASSWORD       = "";
    public static String SYS_USER_NAME  = "";
    public static String SYS_PASSWORD   = "";

    public static Configuration newConfiguration() {
        Configuration conf = new Configuration();
        // OCP mode
        conf.set(HBASE_OCEANBASE_PARAM_URL, PARAM_URL);
        conf.set(HBASE_OCEANBASE_FULL_USER_NAME, FULL_USER_NAME);
        conf.set(HBASE_OCEANBASE_PASSWORD, PASSWORD);
        conf.set(HBASE_OCEANBASE_SYS_USER_NAME, SYS_USER_NAME);
        conf.set(HBASE_OCEANBASE_SYS_PASSWORD, SYS_PASSWORD);

        // ODP mode
        // conf.set(HBASE_OCEANBASE_ODP_ADDR, "xx.xx.xx.xx");
        // conf.setInt(HBASE_OCEANBASE_ODP_PORT, 8080);
        // conf.set(HBASE_OCEANBASE_FULL_USER_NAME, FULL_USER_NAME);
        // conf.set(HBASE_OCEANBASE_PASSWORD, PASSWORD);
        // conf.setBoolean(HBASE_OCEANBASE_ODP_MODE, true);
        // conf.set(HBASE_OCEANBASE_DATABASE, "daseName);
        return conf;
    }

    public static OHTableClient newOHTableClient(String tableName) {
        return new OHTableClient(tableName, newConfiguration());
    }
}
