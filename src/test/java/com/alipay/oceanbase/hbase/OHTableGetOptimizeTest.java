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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE_GLOBAL;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * Test class for HBase Get optimization with MaxVersions=1 on single-level partition tables
 * This test ONLY runs Get optimization related test cases on single-level partition tables
 * Each test case uses its own table instance with single-level partitioning
 */
public class OHTableGetOptimizeTest {

    private Table hTable;

    @BeforeClass
    public static void setupClass() throws Exception {
        // Initialize test environment if needed
    }

    @After
    public void tearDown() throws IOException {
        if (hTable != null) {
            hTable.close();
        }
    }

    @AfterClass
    public static void tearDownClass() throws SQLException {
        ObHTableTestUtil.closeConn();
    }

    /**
     * Test Get optimization on single-level partition table with MaxVersions=1
     * Verifies: Table-level MaxVersions=1 and setMaxVersions(1) both return only latest version
     */
    @Test
    public void testGetOptimizeWithMaxVersion1() throws Exception {
        // Initialize table - single-level KEY partition with MaxVersions=1
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize");

        String family1 = "family_max_version_1";
        String family2 = "family_max_version_default";
        String key1 = "optimize_key_001";
        String key2 = "optimize_key_002";
        String column1 = "col1";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";

        // Scenario 1: Table with MaxVersions=1, single column specified
        long t1 = System.currentTimeMillis();
        Put put = new Put(toBytes(key1));
        put.addColumn(family1.getBytes(), column1.getBytes(), t1, toBytes(value1));
        hTable.put(put);

        Thread.sleep(10);
        long t2 = System.currentTimeMillis();
        put = new Put(toBytes(key1));
        put.addColumn(family1.getBytes(), column1.getBytes(), t2, toBytes(value2));
        hTable.put(put);

        Thread.sleep(10);
        long t3 = System.currentTimeMillis();
        put = new Put(toBytes(key1));
        put.addColumn(family1.getBytes(), column1.getBytes(), t3, toBytes(value3));
        hTable.put(put);

        // Get with single column
        Get get = new Get(toBytes(key1));
        get.addColumn(family1.getBytes(), column1.getBytes());
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals(value3, Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t3, r.rawCells()[0].getTimestamp());

        // Test min(1, 10) = 1
        get = new Get(toBytes(key1));
        get.addColumn(family1.getBytes(), column1.getBytes());
        get.setMaxVersions(10);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals(value3, Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));

        // Get with setMaxVersions(1)
        put = new Put(toBytes(key2));
        put.addColumn(family2.getBytes(), column1.getBytes(), t1, toBytes(value1));
        hTable.put(put);

        put = new Put(toBytes(key2));
        put.addColumn(family2.getBytes(), column1.getBytes(), t2, toBytes(value2));
        hTable.put(put);

        put = new Put(toBytes(key2));
        put.addColumn(family2.getBytes(), column1.getBytes(), t3, toBytes(value3));
        hTable.put(put);

        // Get with setMaxVersions(1)
        get = new Get(toBytes(key2));
        get.addColumn(family2.getBytes(), column1.getBytes());
        get.setMaxVersions(1);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals(value3, Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t3, r.rawCells()[0].getTimestamp());

        // Verify multiple versions exist when requesting them
        get = new Get(toBytes(key2));
        get.addColumn(family2.getBytes(), column1.getBytes());
        get.setMaxVersions(10);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        Assert.assertTrue(r.rawCells().length >= 3);
        Assert.assertEquals(value3, Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(value2, Bytes.toString(r.rawCells()[1].getValueArray(), r.rawCells()[1].getValueOffset(), r.rawCells()[1].getValueLength()));
        Assert.assertEquals(value1, Bytes.toString(r.rawCells()[2].getValueArray(), r.rawCells()[2].getValueOffset(), r.rawCells()[2].getValueLength()));
    }

    /**
     * Test Get optimization with single column query
     * Verifies: Table MaxVersions=1 returns only latest version for single column
     */
    @Test
    public void testGetOptimizeSingleColumn() throws Exception {
        // Initialize table - single-level KEY partition with MaxVersions=1
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_t");

        String family = "family_max_version_1";
        String key = "single_col_key";
        String col1 = "column1";
        long t1 = System.currentTimeMillis();


        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t1, toBytes("value_c1_v1"));
        hTable.put(put);

        Thread.sleep(10);
        long t2 = System.currentTimeMillis();
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t2, toBytes("value_c1_v2"));
        hTable.put(put);

        Thread.sleep(10);
        long t3 = System.currentTimeMillis();
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t3, toBytes("value_c1_v3"));
        hTable.put(put);


        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col1.getBytes());
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_c1_v3", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t3, r.rawCells()[0].getTimestamp());


        String col2 = "column2";
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col2.getBytes(), t1, toBytes("value_c2_v1"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col2.getBytes(), t2, toBytes("value_c2_v2"));
        hTable.put(put);


        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col2.getBytes());
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r2 = hTable.get(get);
        Assert.assertEquals(1, r2.rawCells().length);
        Assert.assertEquals("value_c2_v2", Bytes.toString(r2.rawCells()[0].getValueArray(), r2.rawCells()[0].getValueOffset(), r2.rawCells()[0].getValueLength()));
    }

    /**
     * Test Get optimization with batch Get operations
     * Verifies: Batch Get correctly returns latest version for each key
     */
    @Test
    public void testGetOptimizeBatchGet() throws Exception {
        // Initialize table - single-level KEY partition
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize");

        String family = "family_max_version_1";
        List<String> keys = Arrays.asList("batch_key_01", "batch_key_02", "batch_key_03");
        String column = "batch_col";
        long t1 = System.currentTimeMillis();


        for (String key : keys) {
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), column.getBytes(), t1, toBytes(key + "_v1"));
            hTable.put(put);

            Thread.sleep(5);
            long t2 = System.currentTimeMillis();
            put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), column.getBytes(), t2, toBytes(key + "_v2"));
            hTable.put(put);

            Thread.sleep(5);
            long t3 = System.currentTimeMillis();
            put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), column.getBytes(), t3, toBytes(key + "_v3"));
            hTable.put(put);
        }

        // Batch Get
        List<Get> gets = new ArrayList<>();
        for (String key : keys) {
            Get get = new Get(toBytes(key));
            get.addColumn(family.getBytes(), column.getBytes());
            gets.add(get);
        }

        for (Get get : gets) {
            get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        }

        Result[] results = hTable.get(gets);
        Assert.assertEquals(3, results.length);
        for (int i = 0; i < results.length; i++) {
            Result r = results[i];
            Assert.assertEquals(1, r.rawCells().length);

            Assert.assertEquals(keys.get(i) + "_v3", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        }
    }

    /**
     * Test Get optimization with explicit setMaxVersions(1)
     * Verifies: setMaxVersions(1) returns latest version, different values return multiple versions
     */
    @Test
    public void testGetOptimizeExplicitMaxVersion1() throws Exception {
        // Initialize table - single-level KEY partition
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_t");

        String family = "family_max_version_default";
        String key = "explicit_max_v1_key";
        String column = "col";
        long baseTime = System.currentTimeMillis();


        for (int i = 1; i <= 5; i++) {
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), column.getBytes(), baseTime + i, toBytes("value_" + i));
            hTable.put(put);
            Thread.sleep(5);
        }

        // Get with setMaxVersions(1)
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setMaxVersions(1);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_5", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));

        // Get with setMaxVersions(3)
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setMaxVersions(3);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        Assert.assertEquals(3, r.rawCells().length);
        Assert.assertEquals("value_5", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals("value_4", Bytes.toString(r.rawCells()[1].getValueArray(), r.rawCells()[1].getValueOffset(), r.rawCells()[1].getValueLength()));
        Assert.assertEquals("value_3", Bytes.toString(r.rawCells()[2].getValueArray(), r.rawCells()[2].getValueOffset(), r.rawCells()[2].getValueLength()));

        // Get with default MaxVersions
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_5", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
    }

    /**
     * Test Get optimization with time range filtering
     * Verifies: Time range correctly filters results with MaxVersions=1
     */
    @Test
    public void testGetOptimizeWithTimeRange() throws Exception {
        // Initialize table - single-level KEY partition
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_t");

        String family = "family_max_version_1";
        String key = "time_range_key";
        String column = "col";
        long t1 = System.currentTimeMillis();


        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t1, toBytes("value_t1"));
        hTable.put(put);

        Thread.sleep(100);
        long t2 = System.currentTimeMillis();
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t2, toBytes("value_t2"));
        hTable.put(put);

        Thread.sleep(100);
        long t3 = System.currentTimeMillis();
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t3, toBytes("value_t3"));
        hTable.put(put);

        // Get with time range (t1 to t2+1)
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setTimeRange(t1, t2 + 1);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        // With MaxVersions=1 at table level, should get only 1 version
        Assert.assertTrue(r.rawCells().length >= 1);
        // The result should be within the time range
        Assert.assertTrue(r.rawCells()[0].getTimestamp() >= t1 && r.rawCells()[0].getTimestamp() <= t2);

        // Get with time range (t1 to t3+1) and setMaxVersions(1)
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setTimeRange(t1, t3 + 1);
        get.setMaxVersions(1);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_t3", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
    }

    /**
     * Test Get optimization with specific timestamp
     * Verifies: setTimestamp accurately retrieves data at exact timestamp, handles non-existent timestamp
     */
    @Test
    public void testGetOptimizeWithSpecificTimestamp() throws Exception {
        // Initialize table - single-level KEY partition
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize");

        String family1 = "family_max_version_1";
        String family2 = "family_max_version_default";
        String key = "specific_ts_key";
        String column = "col";

        long t1 = System.currentTimeMillis();
        Thread.sleep(10);
        long t2 = System.currentTimeMillis();
        Thread.sleep(10);
        long t3 = System.currentTimeMillis();

        // Test with MaxVersions=1 table
        Put put = new Put(toBytes(key));
        put.addColumn(family1.getBytes(), column.getBytes(), t1, toBytes("value_t1"));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key));
        put.addColumn(family1.getBytes(), column.getBytes(), t2, toBytes("value_t2"));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key));
        put.addColumn(family1.getBytes(), column.getBytes(), t3, toBytes("value_t3"));
        hTable.put(put);

        // Get with specific timestamp t2
        Get get = new Get(toBytes(key));
        get.addColumn(family1.getBytes(), column.getBytes());
        get.setTimeStamp(t2);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_t2", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t2, r.rawCells()[0].getTimestamp());

        // Get with specific timestamp t1
        get = new Get(toBytes(key));
        get.addColumn(family1.getBytes(), column.getBytes());
        get.setTimeStamp(t1);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_t1", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t1, r.rawCells()[0].getTimestamp());

        // Get with specific timestamp t3
        get = new Get(toBytes(key));
        get.addColumn(family1.getBytes(), column.getBytes());
        get.setTimeStamp(t3);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_t3", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t3, r.rawCells()[0].getTimestamp());

        // Test with default MaxVersions table
        String key2 = "specific_ts_key2";
        put = new Put(toBytes(key2));
        put.addColumn(family2.getBytes(), column.getBytes(), t1, toBytes("value2_t1"));
        hTable.put(put);

        put = new Put(toBytes(key2));
        put.addColumn(family2.getBytes(), column.getBytes(), t2, toBytes("value2_t2"));
        hTable.put(put);

        put = new Put(toBytes(key2));
        put.addColumn(family2.getBytes(), column.getBytes(), t3, toBytes("value2_t3"));
        hTable.put(put);

        // Get with setTimestamp + setMaxVersions(1)
        get = new Get(toBytes(key2));
        get.addColumn(family2.getBytes(), column.getBytes());
        get.setTimeStamp(t2);
        get.setMaxVersions(1);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value2_t2", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t2, r.rawCells()[0].getTimestamp());

        // Get with non-existent timestamp
        long nonExistentTs = t1 - 1000;
        get = new Get(toBytes(key2));
        get.addColumn(family2.getBytes(), column.getBytes());
        get.setTimeStamp(nonExistentTs);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);
    }

    /**
     * Test Get with specific timestamp across multiple versions
     * Verifies: Retrieves exact version at specified timestamp, returns empty for non-existent timestamp
     */
    @Test
    public void testGetOptimizeWithSpecificTimestampMultiVersions() throws Exception {
        // Initialize table - single-level KEY partition
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_t");

        String family = "family_max_version_default";
        String key = "specific_ts_multi_key";
        String column = "col";

        long baseTime = System.currentTimeMillis();
        long[] timestamps = new long[5];
        for (int i = 0; i < 5; i++) {
            timestamps[i] = baseTime + (i * 100);
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), column.getBytes(), timestamps[i], toBytes("value_" + i));
            hTable.put(put);
            Thread.sleep(5);
        }

        // Get with specific timestamp
        for (int i = 0; i < 5; i++) {
            Get get = new Get(toBytes(key));
            get.addColumn(family.getBytes(), column.getBytes());
            get.setTimeStamp(timestamps[i]);
            get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
            Result r = hTable.get(get);
            Assert.assertEquals(1, r.rawCells().length);
            Assert.assertEquals("value_" + i, Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
            Assert.assertEquals(timestamps[i], r.rawCells()[0].getTimestamp());
        }

        // Get with setTimestamp + setMaxVersions(1)
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setTimeStamp(timestamps[2]);
        get.setMaxVersions(1);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_2", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(timestamps[2], r.rawCells()[0].getTimestamp());

        // Get with non-existent timestamp between two versions
        long betweenTs = timestamps[2] + 50;
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setTimeStamp(betweenTs);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);
    }

    /**
     * Test Get optimization with multiple qualifiers on MaxVersions=1 table
     * Verifies: Each qualifier returns only latest version when table MaxVersions=1
     */
    @Test
    public void testGetOptimizeWithMultipleQualifiersOnMaxVersion1Table() throws Exception {
        // Initialize table - single-level KEY partition with MaxVersions=1
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize");

        String family = "family_max_version_1";
        String key = "multi_qual_key_001";
        String col1 = "qualifier1";
        String col2 = "qualifier2";
        String col3 = "qualifier3";

        long t1 = System.currentTimeMillis();
        Thread.sleep(10);
        long t2 = System.currentTimeMillis();
        Thread.sleep(10);
        long t3 = System.currentTimeMillis();

        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t1, toBytes("col1_v1"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t2, toBytes("col1_v2"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t3, toBytes("col1_v3"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col2.getBytes(), t1, toBytes("col2_v1"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col2.getBytes(), t2, toBytes("col2_v2"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col2.getBytes(), t3, toBytes("col2_v3"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col3.getBytes(), t1, toBytes("col3_v1"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col3.getBytes(), t2, toBytes("col3_v2"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col3.getBytes(), t3, toBytes("col3_v3"));
        hTable.put(put);

        // Get with multiple qualifiers
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col1.getBytes());
        get.addColumn(family.getBytes(), col2.getBytes());
        get.addColumn(family.getBytes(), col3.getBytes());
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        
        Assert.assertEquals(3, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength());
            resultMap.put(qualifier, value);
        }
        
        Assert.assertEquals("col1_v3", resultMap.get(col1));
        Assert.assertEquals("col2_v3", resultMap.get(col2));
        Assert.assertEquals("col3_v3", resultMap.get(col3));
    }

    /**
     * Test Get optimization with multiple qualifiers and setMaxVersions(1)
     * Verifies: setMaxVersions(1) returns latest version per qualifier, higher values return multiple versions
     */
    @Test
    public void testGetOptimizeWithMultipleQualifiersAndExplicitMaxVersion1() throws Exception {
        // Initialize table - single-level KEY partition with default MaxVersions
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_t");

        String family = "family_max_version_default";
        String key = "multi_qual_key_002";
        String col1 = "q1";
        String col2 = "q2";
        String col3 = "q3";
        String col4 = "q4";

        long baseTime = System.currentTimeMillis();

        // Insert 5 versions for each qualifier
        for (int i = 1; i <= 5; i++) {
            long ts = baseTime + (i * 10);
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), col1.getBytes(), ts, toBytes("q1_value_" + i));
            put.addColumn(family.getBytes(), col2.getBytes(), ts, toBytes("q2_value_" + i));
            put.addColumn(family.getBytes(), col3.getBytes(), ts, toBytes("q3_value_" + i));
            put.addColumn(family.getBytes(), col4.getBytes(), ts, toBytes("q4_value_" + i));
            hTable.put(put);
            Thread.sleep(5);
        }

        // Get with setMaxVersions(1)
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col1.getBytes());
        get.addColumn(family.getBytes(), col2.getBytes());
        get.addColumn(family.getBytes(), col3.getBytes());
        get.addColumn(family.getBytes(), col4.getBytes());
        get.setMaxVersions(1);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        
        Assert.assertEquals(4, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength());
            resultMap.put(qualifier, value);
        }
        
        Assert.assertEquals("q1_value_5", resultMap.get(col1));
        Assert.assertEquals("q2_value_5", resultMap.get(col2));
        Assert.assertEquals("q3_value_5", resultMap.get(col3));
        Assert.assertEquals("q4_value_5", resultMap.get(col4));

        // Get with setMaxVersions(3)
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col1.getBytes());
        get.addColumn(family.getBytes(), col2.getBytes());
        get.setMaxVersions(3);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        
        Assert.assertEquals(6, r.rawCells().length);
    }

    /**
     * Test Get optimization with multiple qualifiers and specific timestamp
     * Verifies: All qualifiers return data at specified timestamp
     */
    @Test
    public void testGetOptimizeWithMultipleQualifiersAndTimestamp() throws Exception {
        // Initialize table - single-level KEY partition with MaxVersions=1
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize");

        String family = "family_max_version_1";
        String key = "multi_qual_ts_key";
        String col1 = "qualifier_a";
        String col2 = "qualifier_b";
        String col3 = "qualifier_c";

        long t1 = System.currentTimeMillis();
        Thread.sleep(10);
        long t2 = System.currentTimeMillis();
        Thread.sleep(10);
        long t3 = System.currentTimeMillis();

        // Insert data at different timestamps
        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t1, toBytes("a_t1"));
        put.addColumn(family.getBytes(), col2.getBytes(), t1, toBytes("b_t1"));
        put.addColumn(family.getBytes(), col3.getBytes(), t1, toBytes("c_t1"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t2, toBytes("a_t2"));
        put.addColumn(family.getBytes(), col2.getBytes(), t2, toBytes("b_t2"));
        put.addColumn(family.getBytes(), col3.getBytes(), t2, toBytes("c_t2"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t3, toBytes("a_t3"));
        put.addColumn(family.getBytes(), col2.getBytes(), t3, toBytes("b_t3"));
        put.addColumn(family.getBytes(), col3.getBytes(), t3, toBytes("c_t3"));
        hTable.put(put);

        // Get with multiple qualifiers and specific timestamp
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col1.getBytes());
        get.addColumn(family.getBytes(), col2.getBytes());
        get.addColumn(family.getBytes(), col3.getBytes());
        get.setTimeStamp(t2);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        
        Assert.assertEquals(3, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength());
            long timestamp = r.rawCells()[i].getTimestamp();
            resultMap.put(qualifier, value);
            Assert.assertEquals(t2, timestamp);
        }
        
        Assert.assertEquals("a_t2", resultMap.get(col1));
        Assert.assertEquals("b_t2", resultMap.get(col2));
        Assert.assertEquals("c_t2", resultMap.get(col3));
    }

    /**
     * Test min(table_max_version, setMaxVersions) rule with single qualifier
     * Verifies: Returns min(1, N) = 1 version when table MaxVersions=1 regardless of client request
     */
    @Test
    public void testTableMaxVersionPrecedence() throws Exception {
        // Initialize table - single-level KEY partition with MaxVersions=1
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize");

        String family = "family_max_version_1";
        String key = "precedence_test_key";
        String col = "test_col";

        long baseTime = System.currentTimeMillis();


        for (int i = 1; i <= 5; i++) {
            long ts = baseTime + (i * 10);
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), col.getBytes(), ts, toBytes("value_" + i));
            hTable.put(put);
            Thread.sleep(5);
        }

        // Test: min(1, 3) = 1
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col.getBytes());
        get.setMaxVersions(3);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_5", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));

        // Test: min(1, 5) = 1
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col.getBytes());
        get.setMaxVersions(5);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_5", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));

        // Test: min(1, 10) = 1
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col.getBytes());
        get.setMaxVersions(10);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_5", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
    }

    /**
     * Test min(table_max_version, setMaxVersions) rule with multiple qualifiers
     * Verifies: Each qualifier returns min(1, 3) = 1 version independently
     */
    @Test
    public void testTableMaxVersionPrecedenceWithMultipleQualifiers() throws Exception {
        // Initialize table - single-level KEY partition with MaxVersions=1
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize");

        String family = "family_max_version_1";
        String key = "multi_qual_precedence_key";
        String col1 = "q1";
        String col2 = "q2";
        String col3 = "q3";

        long baseTime = System.currentTimeMillis();

        for (int i = 1; i <= 5; i++) {
            long ts = baseTime + (i * 10);
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), col1.getBytes(), ts, toBytes("q1_v" + i));
            put.addColumn(family.getBytes(), col2.getBytes(), ts, toBytes("q2_v" + i));
            put.addColumn(family.getBytes(), col3.getBytes(), ts, toBytes("q3_v" + i));
            hTable.put(put);
            Thread.sleep(5);
        }

        // Test: min(1, 3) = 1 per qualifier
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col1.getBytes());
        get.addColumn(family.getBytes(), col2.getBytes());
        get.addColumn(family.getBytes(), col3.getBytes());
        get.setMaxVersions(3);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        
        Assert.assertEquals(3, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength());
            resultMap.put(qualifier, value);
        }
        
        Assert.assertEquals("q1_v5", resultMap.get(col1));
        Assert.assertEquals("q2_v5", resultMap.get(col2));
        Assert.assertEquals("q3_v5", resultMap.get(col3));
    }

    /**
     * Test Get without specifying qualifiers on MaxVersions=1 table
     * Verifies: Returns latest version of all qualifiers
     */
    @Test
    public void testGetOptimizeWithoutQualifierOnMaxVersion1Table() throws Exception {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize");

        String family = "family_max_version_1";
        String key = "no_qualifier_max_v1_key";
        String col1 = "qualifier_1";
        String col2 = "qualifier_2";
        String col3 = "qualifier_3";

        long baseTime = System.currentTimeMillis();

        for (int i = 1; i <= 5; i++) {
            long ts = baseTime + (i * 10);
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), col1.getBytes(), ts, toBytes("col1_value_" + i));
            put.addColumn(family.getBytes(), col2.getBytes(), ts, toBytes("col2_value_" + i));
            put.addColumn(family.getBytes(), col3.getBytes(), ts, toBytes("col3_value_" + i));
            hTable.put(put);
            Thread.sleep(5);
        }

        Get get = new Get(toBytes(key));
        get.addFamily(family.getBytes());
        Result r = hTable.get(get);
        
        Assert.assertEquals(3, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength());
            resultMap.put(qualifier, value);
        }
        
        Assert.assertEquals("col1_value_5", resultMap.get(col1));
        Assert.assertEquals("col2_value_5", resultMap.get(col2));
        Assert.assertEquals("col3_value_5", resultMap.get(col3));
    }

    /**
     * Test Get without specifying qualifiers with setMaxVersions(1)
     * Verifies: Returns latest version of all qualifiers when setMaxVersions(1)
     */
    @Test
    public void testGetOptimizeWithoutQualifierAndExplicitMaxVersion1() throws Exception {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_t");

        String family = "family_max_version_default";
        String key = "no_qualifier_explicit_v1_key";
        String col1 = "q1";
        String col2 = "q2";
        String col3 = "q3";
        String col4 = "q4";

        long baseTime = System.currentTimeMillis();

        for (int i = 1; i <= 5; i++) {
            long ts = baseTime + (i * 10);
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), col1.getBytes(), ts, toBytes("q1_v" + i));
            put.addColumn(family.getBytes(), col2.getBytes(), ts, toBytes("q2_v" + i));
            put.addColumn(family.getBytes(), col3.getBytes(), ts, toBytes("q3_v" + i));
            put.addColumn(family.getBytes(), col4.getBytes(), ts, toBytes("q4_v" + i));
            hTable.put(put);
            Thread.sleep(5);
        }

        Get get = new Get(toBytes(key));
        get.addFamily(family.getBytes());
        get.setMaxVersions(1);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        
        Assert.assertEquals(4, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength());
            resultMap.put(qualifier, value);
        }
        
        Assert.assertEquals("q1_v5", resultMap.get(col1));
        Assert.assertEquals("q2_v5", resultMap.get(col2));
        Assert.assertEquals("q3_v5", resultMap.get(col3));
        Assert.assertEquals("q4_v5", resultMap.get(col4));
    }

    /**
     * Test Get without specifying qualifiers with specific timestamp
     * Verifies: All qualifiers return data at specified timestamp
     */
    @Test
    public void testGetOptimizeWithoutQualifierAndTimestamp() throws Exception {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize");

        String family = "family_max_version_1";
        String key = "no_qualifier_ts_key";
        String col1 = "qa";
        String col2 = "qb";
        String col3 = "qc";

        long t1 = System.currentTimeMillis();
        Thread.sleep(10);
        long t2 = System.currentTimeMillis();
        Thread.sleep(10);
        long t3 = System.currentTimeMillis();

        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t1, toBytes("qa_t1"));
        put.addColumn(family.getBytes(), col2.getBytes(), t1, toBytes("qb_t1"));
        put.addColumn(family.getBytes(), col3.getBytes(), t1, toBytes("qc_t1"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t2, toBytes("qa_t2"));
        put.addColumn(family.getBytes(), col2.getBytes(), t2, toBytes("qb_t2"));
        put.addColumn(family.getBytes(), col3.getBytes(), t2, toBytes("qc_t2"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t3, toBytes("qa_t3"));
        put.addColumn(family.getBytes(), col2.getBytes(), t3, toBytes("qb_t3"));
        put.addColumn(family.getBytes(), col3.getBytes(), t3, toBytes("qc_t3"));
        hTable.put(put);

        Get get = new Get(toBytes(key));
        get.addFamily(family.getBytes());
        get.setTimeStamp(t2);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        
        Assert.assertEquals(3, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength());
            long timestamp = r.rawCells()[i].getTimestamp();
            resultMap.put(qualifier, value);
            Assert.assertEquals(t2, timestamp);
        }
        
        Assert.assertEquals("qa_t2", resultMap.get(col1));
        Assert.assertEquals("qb_t2", resultMap.get(col2));
        Assert.assertEquals("qc_t2", resultMap.get(col3));
    }

    /**
     * Test Get without specifying qualifiers with timestamp and setMaxVersions(1)
     * Verifies: Returns exact version at specified timestamp for all qualifiers
     */
    @Test
    public void testGetOptimizeWithoutQualifierTimestampAndMaxVersion1() throws Exception {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_t");

        String family = "family_max_version_default";
        String key = "no_qualifier_ts_v1_key";
        String col1 = "qualifier_a";
        String col2 = "qualifier_b";
        String col3 = "qualifier_c";
        String col4 = "qualifier_d";

        long baseTime = System.currentTimeMillis();
        long[] timestamps = new long[5];
        for (int i = 0; i < 5; i++) {
            timestamps[i] = baseTime + (i * 100);
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), col1.getBytes(), timestamps[i], toBytes("a_v" + i));
            put.addColumn(family.getBytes(), col2.getBytes(), timestamps[i], toBytes("b_v" + i));
            put.addColumn(family.getBytes(), col3.getBytes(), timestamps[i], toBytes("c_v" + i));
            put.addColumn(family.getBytes(), col4.getBytes(), timestamps[i], toBytes("d_v" + i));
            hTable.put(put);
            Thread.sleep(5);
        }

        Get get = new Get(toBytes(key));
        get.addFamily(family.getBytes());
        get.setTimeStamp(timestamps[2]);
        get.setMaxVersions(1);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r = hTable.get(get);
        
        Assert.assertEquals(4, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength());
            long timestamp = r.rawCells()[i].getTimestamp();
            resultMap.put(qualifier, value);
            Assert.assertEquals(timestamps[2], timestamp);
        }
        
        Assert.assertEquals("a_v2", resultMap.get(col1));
        Assert.assertEquals("b_v2", resultMap.get(col2));
        Assert.assertEquals("c_v2", resultMap.get(col3));
        Assert.assertEquals("d_v2", resultMap.get(col4));

        // Verify another timestamp
        get = new Get(toBytes(key));
        get.addFamily(family.getBytes());
        get.setTimeStamp(timestamps[4]);
        get.setMaxVersions(1);
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r2 = hTable.get(get);
        
        Assert.assertEquals(4, r2.rawCells().length);
        for (int i = 0; i < r2.rawCells().length; i++) {
            Assert.assertEquals(timestamps[4], r2.rawCells()[i].getTimestamp());
        }
    }

    @Test
    public void testGetOptimizeWithGlobalSetting() throws Exception {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        c.set(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE_GLOBAL, "true");
        hTable = new OHTable(c, "test_get_optimize");
        
        String family = "family_max_version_1";
        String key = "global_setting_key";
        String col = "col";

        long baseTime = System.currentTimeMillis();
        for (int i = 1; i <= 5; i++) {
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), col.getBytes(), baseTime + i, toBytes("value_" + i));
            hTable.put(put);
        }

        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col.getBytes());
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "false".getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_5", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));


        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col.getBytes());
        get.setAttribute(HBASE_HTABLE_HOTKEY_GET_OPTIMIZE_ENABLE, "true".getBytes());
        Result r2 = hTable.get(get);
        Assert.assertEquals(1, r2.rawCells().length);
        Assert.assertEquals("value_5", Bytes.toString(r2.rawCells()[0].getValueArray(), r2.rawCells()[0].getValueOffset(), r2.rawCells()[0].getValueLength()));


        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col.getBytes());
        Result r3 = hTable.get(get);
        Assert.assertEquals(1, r3.rawCells().length);
        Assert.assertEquals("value_5", Bytes.toString(r3.rawCells()[0].getValueArray(), r3.rawCells()[0].getValueOffset(), r3.rawCells()[0].getValueLength()));
    }
}
