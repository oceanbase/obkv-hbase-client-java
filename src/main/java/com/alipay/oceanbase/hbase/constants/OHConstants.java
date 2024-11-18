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

package com.alipay.oceanbase.hbase.constants;

/**
 * extends <code>HConstants</code>
 */
public final class OHConstants {

    /**
     * ocenbase hbase root server http url
     */
    public static final String   HBASE_OCEANBASE_PARAM_URL                   = "hbase.oceanbase.paramURL";

    /**
     * ocenbase hbase connect server username
     */
    public static final String   HBASE_OCEANBASE_FULL_USER_NAME              = "hbase.oceanbase.fullUserName";

    /**
     * ocenbase hbase connect server password
     */
    public static final String   HBASE_OCEANBASE_PASSWORD                    = "hbase.oceanbase.password";

    /**
     * oceanbase hbase connect server system username
     */
    public static final String   HBASE_OCEANBASE_SYS_USER_NAME               = "hbase.oceanbase.sysUserName";

    /**
     * oceanbase hbase connect server system password
     */
    public static final String   HBASE_OCEANBASE_SYS_PASSWORD                = "hbase.oceanbase.sysPassword";

    /**
     * ocenbase hbase connect server password
     */
    public static final String   HBASE_OCEANBASE_BATCH_EXECUTOR              = "hbase.oceanbase.batch.executor";

    /**
     * ocenbase hbase connect server ODP address
     */
    public static final String   HBASE_OCEANBASE_ODP_ADDR                    = "hbase.oceanbase.odpAddr";

    /**
     * ocenbase hbase connect server ODP port
     */
    public static final String   HBASE_OCEANBASE_ODP_PORT                    = "hbase.oceanbase.odpPort";

    /**
     * ocenbase hbase connect server ODP mode
     */
    public static final String   HBASE_OCEANBASE_ODP_MODE                    = "hbase.oceanbase.odpMode";

    /**
     * ocenbase hbase connect server database
     */
    public static final String   HBASE_OCEANBASE_DATABASE                    = "hbase.oceanbase.database";

    /**
     * ocenbase hbase model is consist of following columns
     * K hbase row key
     * Q hbase qualifier
     * T hbase timeStamp
     * V hbase value
     */
    public static final String[] ALL_COLUMNS                                 = new String[] { "K",
            "Q", "T", "V"                                                   };

    /**
     * ocenbase hbase model rowkey column is consist of following column
     * K, Q, T hbase value
     */
    public static final String[] ROW_KEY_COLUMNS                             = new String[] { "K",
            "Q", "T"                                                        };

    /**
     * ocenbase hbase model value column is consist of following column
     * V hbase value
     */
    public static final String[] V_COLUMNS                                   = new String[] { "V" };

    public static final String   HBASE_HTABLE_POOL_SEPERATOR                 = "$";

    /**
     * internal attribute of ohtable pool to optimize auto-flush attribute for each table
     */
    public static final String   HBASE_HTABLE_POOL_AUTO_FLUSH                = "hbase.htable.pool.auto.flush";

    /**
     * internal attribute of ohtable pool to optimize clear-buffer-on-fail attribute for each table
     */
    public static final String   HBASE_HTABLE_POOL_CLEAR_BUFFER_ON_FAIL      = "hbase.htable.pool.clear.buffer.on.fail";

    /**
     * internal attribute of ohtable pool to optimize write-buffer-size attribute for each table
     */
    public static final String   HBASE_HTABLE_POOL_WRITE_BUFFER_SIZE         = "hbase.htable.pool.write.buffer.size";

    /**
     * internal attribute of ohtable pool to optimize auto-flush attribute for each table
     */
    public static final String   HBASE_HTABLE_POOL_OPERATION_TIMEOUT         = "hbase.htable.pool.operation.timeout";

    /**
     * internal attribute of ohtable pool which enable the test load
     */
    public static final String   HBASE_HTABLE_TEST_LOAD_ENABLE               = "hbase.htable.test.load.enable";

    /**
     * internal attribute of ohtable pool which specify the test load suffix
     */
    public static final String   HBASE_HTABLE_TEST_LOAD_SUFFIX               = "hbase.htable.test.load.suffix";

    /**
     * the default value of internal attribute of ohtable pool which specify the test load suffix
     */
    public static final String   DEFAULT_HBASE_HTABLE_TEST_LOAD_SUFFIX       = "_t";

    /*-------------------------------------------------------------------------------------------------------------*/

    /**
     * following constants are copied from hbase for compatibility
     */
    public static final String   HBASE_CLIENT_OPERATION_EXECUTE_IN_POOL      = "hbase.client.operation.executeinpool";

    public static final String   WRITE_BUFFER_SIZE_KEY                       = "hbase.client.write.buffer";

    public static final String   MAX_KEYVALUE_SIZE_KEY                       = "hbase.client.keyvalue.maxsize";

    public static final String   HBASE_HTABLE_PUT_WRITE_BUFFER_CHECK         = "hbase.htable.put.write.buffer.check";

    public static final int      DEFAULT_HBASE_HTABLE_PUT_WRITE_BUFFER_CHECK = 10;

    public static final long     WRITE_BUFFER_SIZE_DEFAULT                   = 2097152L;

    public static final int      MAX_KEYVALUE_SIZE_DEFAULT                   = 10485760;

    public static final String   SOCKET_TIMEOUT                              = "ipc.socket.timeout";

    public static final int      DEFAULT_SOCKET_TIMEOUT                      = 20000;                                   // 20 seconds

}
