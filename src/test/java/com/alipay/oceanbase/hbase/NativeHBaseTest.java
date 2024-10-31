/*-
 * #%L
 * com.oceanbase:obkv-hbase-client
 * %%
 * Copyright (C) 2022 - 2024 OceanBase Group
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

import com.alipay.oceanbase.hbase.util.NativeHBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;

public class NativeHBaseTest extends HTableTestBase {

    static Admin     admin;
    static TableName tableName1 = TableName.valueOf("test");
    static TableName tableName2 = TableName.valueOf("test_multi_cf");

    static {
        try {
            admin = NativeHBaseUtil.getAdmin();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setup() throws IOException {
        hTable = NativeHBaseUtil.getTable(tableName1);
        multiCfHTable = NativeHBaseUtil.getTable(tableName2);
    }

    @Before
    public void cleanData() throws IOException {
        admin.disableTable(tableName1);
        admin.disableTable(tableName2);
        admin.truncateTable(tableName1, true);
        admin.truncateTable(tableName2, true);
    }

    @AfterClass
    public static void finish() throws IOException {
        hTable.close();
        multiCfHTable.close();
    }

}
