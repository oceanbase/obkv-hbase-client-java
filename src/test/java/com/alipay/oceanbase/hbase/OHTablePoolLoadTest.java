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

import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import com.alipay.oceanbase.rpc.exception.ObTableNotExistException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.junit.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OHTablePoolLoadTest extends HTableTestBase {
    private static OHTablePool ohTablePool;

    @BeforeClass
    public static void setup() throws Exception {
        Configuration c = new Configuration();
        c.set(HBASE_HTABLE_TEST_LOAD_ENABLE, "true");
        ohTablePool = new OHTablePool(c, 10);

        ohTablePool.setFullUserName("test", ObHTableTestUtil.FULL_USER_NAME);
        ohTablePool.setPassword("test", ObHTableTestUtil.PASSWORD);
        if (ObHTableTestUtil.ODP_MODE) {
            ohTablePool.setOdpAddr("test", ObHTableTestUtil.ODP_ADDR);
            ohTablePool.setOdpPort("test", ObHTableTestUtil.ODP_PORT);
            ohTablePool.setOdpMode("test", ObHTableTestUtil.ODP_MODE);
            ohTablePool.setDatabase("test", ObHTableTestUtil.DATABASE);
        } else {
            ohTablePool.setParamUrl("test", ObHTableTestUtil.PARAM_URL);
            ohTablePool.setSysUserName("test", ObHTableTestUtil.SYS_USER_NAME);
            ohTablePool.setSysPassword("test", ObHTableTestUtil.SYS_PASSWORD);
        }
        ohTablePool.setRuntimeBatchExecutor("test", Executors.newFixedThreadPool(3));
        hTable = ohTablePool.getTable("test");
        multiCfHTable = ohTablePool.getTable("test_multi_cf");
        List<String> tableGroups = new LinkedList<>();
        tableGroups.add("test");
        tableGroups.add("test_multi_cf");
        ObHTableTestUtil.prepareClean(tableGroups);
    }

    @Before
    public void prepareCase() {
        ObHTableTestUtil.cleanData();
    }

    @Test
    public void testRefreshTableEntry() throws Exception {
        ohTablePool.refreshTableEntry("test", "testload", false);

        try {
            ohTablePool.refreshTableEntry("test", "testload", true);
        } catch (Exception e) {
            Throwable t = e;
            while (t.getCause() != null) {
                t = t.getCause();
            }
            Assert.assertTrue(t instanceof ObTableNotExistException);
            Assert.assertTrue(t.getMessage().contains("test_t$testload"));
        }

    }

    @Test
    public void testTestLoadNotExist() throws IOException {
        String column = "existColumn";
        String key = "existKey";

        Delete delete = new Delete(key.getBytes());
        delete.addColumns("testload".getBytes(), column.getBytes());
        hTable.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_ENABLE, "true");
        try {
            delete.addColumns("testload".getBytes(), column.getBytes());
            hTable.delete(delete);
        } catch (IOException e) {
            Throwable t = e;
            while (t.getCause() != null) {
                t = t.getCause();
            }
            if (ObHTableTestUtil.ODP_MODE) {
                Assert.assertTrue(t.getMessage().contains("OB_TABLE_NOT_EXIST"));
            } else {
                Assert.assertTrue(t instanceof ObTableNotExistException);
                Assert.assertTrue(t.getMessage().contains("test_t$testload"));
            }
        }
        hTable.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_SUFFIX, "_a");
        try {
            delete.addColumns("testload".getBytes(), column.getBytes());
            hTable.delete(delete);
        } catch (IOException e) {
            Throwable t = e;
            while (t.getCause() != null) {
                t = t.getCause();
            }
            if (ObHTableTestUtil.ODP_MODE) {
                Assert.assertTrue(t.getMessage().contains("OB_TABLE_NOT_EXIST"));
            } else {
                Assert.assertTrue(t instanceof ObTableNotExistException);
                Assert.assertTrue(t.getMessage().contains("test_a$testload"));
            }
        }

        hTable.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_ENABLE, "false");
        hTable.delete(delete);
    }

    @Test
    public void testNew() throws IOException {
        OHTablePool ohTablePool2 = new OHTablePool();
        ohTablePool2.setFullUserName("test", ObHTableTestUtil.FULL_USER_NAME);
        ohTablePool2.setPassword("test", ObHTableTestUtil.PASSWORD);
        if (ObHTableTestUtil.ODP_MODE) {
            // ODP mode
            ohTablePool2.setOdpAddr("test", ObHTableTestUtil.ODP_ADDR);
            ohTablePool2.setOdpPort("test", ObHTableTestUtil.ODP_PORT);
            ohTablePool2.setOdpMode("test", ObHTableTestUtil.ODP_MODE);
            ohTablePool2.setDatabase("test", ObHTableTestUtil.DATABASE);

        } else {
            // OCP mode
            ohTablePool2.setParamUrl("test", ObHTableTestUtil.PARAM_URL);
            ohTablePool2.setSysUserName("test", ObHTableTestUtil.SYS_USER_NAME);
            ohTablePool2.setSysPassword("test", ObHTableTestUtil.SYS_PASSWORD);
        }
        ohTablePool2.setRuntimeBatchExecutor("test", Executors.newFixedThreadPool(3));
        Table hTable2 = ohTablePool2.getTable("test");
        ohTablePool2.putTable(hTable2);
        assertEquals("test", new String(hTable2.getName().getName()));
        hTable2.close();
        assertTrue(true);
    }

    @AfterClass
    public static void finish() throws IOException, SQLException {
        hTable.close();
        multiCfHTable.close();
        ObHTableTestUtil.closeConn();
    }
}
