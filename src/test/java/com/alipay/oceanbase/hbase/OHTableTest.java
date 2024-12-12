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
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableNotExistException;
import com.alipay.sofa.common.thread.SofaThreadPoolExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Table;
import org.junit.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.SynchronousQueue;

import static com.alipay.oceanbase.hbase.util.TableHBaseLoggerFactory.TABLE_HBASE_LOGGER_SPACE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OHTableTest extends HTableTestBase {

    @BeforeClass
    public static void setup() throws Exception {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");

        hTable = new OHTable(c, "test");
        multiCfHTable = new OHTable(c, "test_multi_cf");
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
        ((OHTable) hTable).refreshTableEntry("family1", false);
        ((OHTable) hTable).refreshTableEntry("family1", true);
        ((OHTable) hTable).refreshTableEntry("testload", false);
        try {
            ((OHTable) hTable).refreshTableEntry("testload", true);
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
    public void testNew() throws Exception {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");

        SofaThreadPoolExecutor executor1 = new SofaThreadPoolExecutor(1, 1, 1000, SECONDS,
            new SynchronousQueue<Runnable>(), "OHTableDefaultExecutePool", TABLE_HBASE_LOGGER_SPACE);
        executor1.allowCoreThreadTimeOut(true);
        Table hTable1 = new OHTable(c, "test".getBytes(), executor1);

        try {
            hTable1.getTableDescriptor();
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        ObTableClient client;
        client = new ObTableClient();
        client.setFullUserName(ObHTableTestUtil.FULL_USER_NAME);
        client.setParamURL(ObHTableTestUtil.PARAM_URL);
        client.setPassword(ObHTableTestUtil.PASSWORD);
        client.setSysUserName(ObHTableTestUtil.SYS_USER_NAME);
        client.setSysPassword(ObHTableTestUtil.SYS_PASSWORD);
        client.init();
        SofaThreadPoolExecutor executor2 = new SofaThreadPoolExecutor(1, 1, 1000, SECONDS,
            new SynchronousQueue<Runnable>(), "OHTableDefaultExecutePool", TABLE_HBASE_LOGGER_SPACE);
        executor1.allowCoreThreadTimeOut(true);
        new OHTable("test".getBytes(), client, executor2);
        assertTrue(true);

    }

    @AfterClass
    public static void finish() throws IOException, SQLException {
        hTable.close();
        multiCfHTable.close();
        ObHTableTestUtil.closeConn();
    }

}
