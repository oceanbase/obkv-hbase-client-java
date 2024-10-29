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
import org.junit.*;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OHTableClientTest extends HTableTestBase {
    @BeforeClass
    public static void before() throws Exception {
        hTable = ObHTableTestUtil.newOHTableClient("test");
        //        hTable = ObHTableTestUtil.newOHTableClient("n1:test");
        ((OHTableClient) hTable).init();
        multiCfHTable = ObHTableTestUtil.newOHTableClient("test_multi_cf");
        ((OHTableClient) multiCfHTable).init();
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
        ((OHTableClient) hTable).refreshTableEntry("family1", false);
        ((OHTableClient) hTable).refreshTableEntry("family1", true);
    }

    @Test
    public void testNew() throws Exception {
        OHTableClient hTable2 = ObHTableTestUtil.newOHTableClient("test");
        //        OHTableClient hTable2 = ObHTableTestUtil.newOHTableClient("n1:test");
        hTable2.init();
        hTable2.getConfiguration().set("rs.list.acquire.read.timeout", "10000");

        assertEquals("test", hTable2.getTableNameString());
        //        assertEquals("n1:test", hTable2.getTableNameString());
        assertEquals("test", new String(hTable2.getName().getName()));
        //        assertEquals("n1:test", new String(hTable2.getTableName()));
        hTable2.close();
        assertTrue(true);
    }

    @AfterClass
    public static void finish() throws Exception {
        hTable.close();
        multiCfHTable.close();
        ObHTableTestUtil.closeConn();
    }
}
