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

import com.alipay.oceanbase.rpc.exception.ObTableNotExistException;
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Executors;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OHTablePoolLoadTest extends HTableTestBase {
    private OHTablePool ohTablePool;

    @Before
    public void setup() throws IOException {
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
//        hTable = ohTablePool.getTable("test");
        hTable = ohTablePool.getTable("n1:test");
    }

    @Test
    public void testRefreshTableEntry() throws Exception {
//        ohTablePool.refreshTableEntry("test", "testload", false);
        ohTablePool.refreshTableEntry("n1:test", "testload", false);

        try {
//            ohTablePool.refreshTableEntry("test", "testload", false);
            ohTablePool.refreshTableEntry("n1:test", "testload", true);
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
        delete.deleteColumns("testload".getBytes(), column.getBytes());
        hTable.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_ENABLE, "true");
        try {
            delete.deleteColumns("testload".getBytes(), column.getBytes());
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
            delete.deleteColumns("testload".getBytes(), column.getBytes());
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
        HTableInterface hTable2 = ohTablePool2.getTable("test");
        ohTablePool2.putTable(hTable2);
        assertTrue(hTable2.isAutoFlush());
        hTable2.setAutoFlush(false);
        assertFalse(hTable2.isAutoFlush());
        hTable2.setAutoFlush(true, true);
        assertTrue(hTable2.isAutoFlush());
        hTable2.setWriteBufferSize(10000000L);
        assertEquals(10000000L, hTable2.getWriteBufferSize());
        assertEquals("test", new String(hTable2.getTableName()));
        hTable2.flushCommits();
        hTable2.close();
        assertTrue(true);
    }
}
