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

import org.apache.hadoop.hbase.client.Get;
import org.junit.*;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OHMultiNamespaceTest extends HTableTestBase {
    @Before
    public void before() throws Exception {
        // use self-defined namespace "n1"
        hTable = ObHTableTestUtil.newOHTableClient("n1:test");
        ((OHTableClient) hTable).init();
    }

    @After
    public void finish() throws IOException {
        hTable.close();
    }

    @Test
    public void testRefreshTableEntry() throws Exception {
        ((OHTableClient) hTable).refreshTableEntry("family1", false);
        ((OHTableClient) hTable).refreshTableEntry("family1", true);
    }

    @Test
    public void testGetColumnFamilyNotExists() throws Exception {
        /** family 不存在时提示不友好，*/
        Get get = new Get(("key_c_f").getBytes());
        get.addFamily("family_not_exists".getBytes());
        expectedException.expect(IOException.class);
        expectedException.expectMessage("query table:n1:test family family_not_exists error.");
        hTable.get(get);
    }

    @Test
    public void testNew() throws Exception {
        OHTableClient hTable2 = ObHTableTestUtil.newOHTableClient("n1:test");
        hTable2.init();
        hTable2.getConfiguration().set("rs.list.acquire.read.timeout", "10000");

        assertTrue(hTable2.isAutoFlush());
        hTable2.setAutoFlush(false);
        assertFalse(hTable2.isAutoFlush());
        hTable2.setAutoFlush(true, true);
        assertTrue(hTable2.isAutoFlush());
        hTable2.setWriteBufferSize(10000000L);
        assertEquals(10000000L, hTable2.getWriteBufferSize());
        assertEquals("n1:test", hTable2.getTableNameString());
        assertEquals("n1:test", new String(hTable2.getTableName()));
        hTable2.flushCommits();
        hTable2.close();
        assertTrue(true);
    }

    @After
    public void after() throws IOException {
        hTable.close();
    }
}
