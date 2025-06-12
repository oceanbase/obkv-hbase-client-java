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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
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

    /*
    CREATE TABLEGROUP test_desc SHARDING = 'ADAPTIVE';
    CREATE TABLE `test_desc$family1` (
        `K` varbinary(1024) NOT NULL,
        `Q` varbinary(256) NOT NULL,
        `T` bigint(20) NOT NULL,
        `V` varbinary(1024) DEFAULT NULL,
        PRIMARY KEY (`K`, `Q`, `T`)
    ) TABLEGROUP = test_desc
    KV_ATTRIBUTES ='{"Hbase": {"TimeToLive": 3600, "MaxVersions": 3}}' 
    PARTITION BY RANGE COLUMNS(K) (
        PARTITION p1 VALUES LESS THAN ('c'),
        PARTITION p2 VALUES LESS THAN ('e'),
        PARTITION p3 VALUES LESS THAN ('g'),
        PARTITION p4 VALUES LESS THAN ('i'),
        PARTITION p5 VALUES LESS THAN ('l'),
        PARTITION p6 VALUES LESS THAN ('n'),
        PARTITION p7 VALUES LESS THAN ('p'),
        PARTITION p8 VALUES LESS THAN ('s'),
        PARTITION p9 VALUES LESS THAN ('v'),
        PARTITION p10 VALUES LESS THAN (MAXVALUE)
    );
    
    CREATE TABLE `test_desc$family2` (
        `K` varbinary(1024) NOT NULL,
        `Q` varbinary(256) NOT NULL,
        `T` bigint(20) NOT NULL,
        `V` varbinary(1024) DEFAULT NULL,
        PRIMARY KEY (`K`, `Q`, `T`)
    ) TABLEGROUP = test_desc
    KV_ATTRIBUTES ='{"Hbase": {"TimeToLive": 7200, "MaxVersions": 3}}'
    PARTITION BY RANGE COLUMNS(K) (
        PARTITION p1 VALUES LESS THAN ('c'),
        PARTITION p2 VALUES LESS THAN ('e'),
        PARTITION p3 VALUES LESS THAN ('g'),
        PARTITION p4 VALUES LESS THAN ('i'),
        PARTITION p5 VALUES LESS THAN ('l'),
        PARTITION p6 VALUES LESS THAN ('n'),
        PARTITION p7 VALUES LESS THAN ('p'),
        PARTITION p8 VALUES LESS THAN ('s'),
        PARTITION p9 VALUES LESS THAN ('v'),
        PARTITION p10 VALUES LESS THAN (MAXVALUE)
    );
    */
    @Test
    public void testGetTableDescriptor() throws Exception {
        final String tableNameStr = "test_desc";

        OHTableClient hTable2 = ObHTableTestUtil.newOHTableClient(tableNameStr);
        hTable2.init();
        try {
            {
                HTableDescriptor descriptor1 = hTable2.getTableDescriptor();
                Assert.assertNotNull(descriptor1);
                Assert.assertEquals(2, descriptor1.getColumnFamilyCount());
                Assert.assertTrue(descriptor1.hasFamily("family1".getBytes()));
                Assert.assertTrue(descriptor1.hasFamily("family2".getBytes()));
                Assert.assertFalse(descriptor1.hasFamily("family".getBytes()));
                ColumnFamilyDescriptor family1 = descriptor1.getColumnFamily("family1".getBytes());
                ColumnFamilyDescriptor family2 = descriptor1.getColumnFamily("family2".getBytes());
                Assert.assertEquals(3600, family1.getTimeToLive());
                Assert.assertEquals(7200, family2.getTimeToLive());
            }
            {
                TableDescriptor descriptor2 = hTable2.getDescriptor();
                Assert.assertNotNull(descriptor2);
                Assert.assertEquals(2, descriptor2.getColumnFamilyCount());
                Assert.assertTrue(descriptor2.hasColumnFamily("family1".getBytes()));
                Assert.assertTrue(descriptor2.hasColumnFamily("family2".getBytes()));
                Assert.assertFalse(descriptor2.hasColumnFamily("family".getBytes()));
                ColumnFamilyDescriptor family1 = descriptor2.getColumnFamily("family1".getBytes());
                ColumnFamilyDescriptor family2 = descriptor2.getColumnFamily("family2".getBytes());
                Assert.assertEquals(3600, family1.getTimeToLive());
                Assert.assertEquals(7200, family2.getTimeToLive());
            }
        } finally {
            hTable2.close();
        }
    }

    @AfterClass
    public static void finish() throws Exception {
        hTable.close();
        multiCfHTable.close();
        ObHTableTestUtil.closeConn();
    }
}
