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
import org.apache.hadoop.hbase.client.Delete;
import org.junit.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_TEST_LOAD_ENABLE;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_TEST_LOAD_SUFFIX;

public class OHTableClientTestLoadTest extends HTableTestBase {
    @BeforeClass
    public static void before() throws Exception {
        hTable = ObHTableTestUtil.newOHTableClient("test");
        ((OHTableClient) hTable).init();
        hTable.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_ENABLE, "true");
        multiCfHTable = ObHTableTestUtil.newOHTableClient("test_multi_cf");
        ((OHTableClient) multiCfHTable).init();
        multiCfHTable.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_ENABLE, "true");
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
    public void test_refresh_table_entry() throws Exception {
        ((OHTableClient) hTable).refreshTableEntry("testload", false);
        try {
            ((OHTableClient) hTable).refreshTableEntry("testload", true);
        } catch (Exception e) {
            Throwable t = e;
            while (t.getCause() != null) {
                t = t.getCause();
            }
            Assert.assertTrue(t instanceof ObTableNotExistException);
            Assert.assertTrue(t.getMessage().contains("test_t$testload"));
        }

    }

    @AfterClass
    public static void after() throws Exception {
        try {
            hTable.close();
            multiCfHTable.close();
            ObHTableTestUtil.closeConn();
        } catch (Exception e) {
            Assert.assertSame(e.getClass(), IOException.class);
            Assert.assertTrue(e.getMessage().contains("put table"));
        }
    }

    @Test
    public void test_testload_notexist() throws IOException {
        String column = "existColumn";
        String key = "existKey";
        hTable.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_ENABLE, "false");
        Delete delete = new Delete(key.getBytes());
        delete.deleteColumns("testload".getBytes(), column.getBytes());
        hTable.delete(delete);
        hTable.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_ENABLE, "true");
        try {
            delete = new Delete(key.getBytes());
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
            delete = new Delete(key.getBytes());
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
    }
}
