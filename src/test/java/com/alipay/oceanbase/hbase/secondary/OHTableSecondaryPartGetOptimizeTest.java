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

package com.alipay.oceanbase.hbase.secondary;

import com.alipay.oceanbase.hbase.OHTable;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import com.alipay.oceanbase.hbase.util.TableTemplateManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;

/**
 * Test class for HBase Get optimization with MaxVersions=1 on secondary partition tables
 * This test ONLY runs Get optimization related test cases on secondary partition tables
 * Each test case uses its own table instance with secondary partitioning
 */
public class OHTableSecondaryPartGetOptimizeTest {

    private Table hTable;

    @BeforeClass
    public static void before() throws Exception {
      openDistributedExecute();
    }

    @AfterClass
    public static void finish() throws Exception {
      closeDistributedExecute();
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
     * Test Get optimization on KEY-RANGE secondary partition with single column
     * Verifies: Get returns only latest version across all secondary partitions when MaxVersions defaults to 1
     */
    @Test
    public void testGetOptimizeWithMaxVersion1OnKeyRange() throws Exception {
        // Initialize table - KEY-RANGE secondary partition with default MaxVersions
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_key_range");

        String family1 = "family1";
        String key1 = "sec_optimize_key_001";
        String column1 = "col1";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";

        // Data distributed across partitions: p1(t1=50), p2(t2=150), p3(t3=250)
        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;

        Put put = new Put(toBytes(key1));
        put.addColumn(family1.getBytes(), column1.getBytes(), t1, toBytes(value1));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key1));
        put.addColumn(family1.getBytes(), column1.getBytes(), t2, toBytes(value2));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key1));
        put.addColumn(family1.getBytes(), column1.getBytes(), t3, toBytes(value3));
        hTable.put(put);

        // Get with default MaxVersions=1
        Get get = new Get(toBytes(key1));
        get.addColumn(family1.getBytes(), column1.getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals(value3, Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t3, r.rawCells()[0].getTimestamp());
    }

    /**
     * Test Get optimization on RANGE-KEY secondary partition with single column
     * Verifies: Get returns only latest version across all partitions when MaxVersions defaults to 1
     */
    @Test
    public void testGetOptimizeSingleColumnOnRangeKey() throws Exception {
        // Initialize table - RANGE-KEY secondary partition with default MaxVersions
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_range_key");

        String family = "family1";
        String key = "sec_single_col_key";
        String col1 = "column1";
        
        // Data distributed across partitions: p1(t1=50), p2(t2=150), p3(t3=250)
        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;


        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t1, toBytes("value_c1_v1"));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t2, toBytes("value_c1_v2"));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t3, toBytes("value_c1_v3"));
        hTable.put(put);

        // Get single column
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col1.getBytes());
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
        Result r2 = hTable.get(get);
        Assert.assertEquals(1, r2.rawCells().length);
        Assert.assertEquals("value_c2_v2", Bytes.toString(r2.rawCells()[0].getValueArray(), r2.rawCells()[0].getValueOffset(), r2.rawCells()[0].getValueLength()));
    }

    /**
     * Test Get optimization with batch Get operations on KEY-RANGE secondary partition
     * Verifies: Batch Get correctly returns latest version for each key across partitions
     */
    @Test
    public void testGetOptimizeBatchGetOnKeyRange() throws Exception {
        // Initialize table - KEY-RANGE secondary partition
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_key_range");

        String family = "family1";
        List<String> keys = Arrays.asList("sec_batch_key_01", "sec_batch_key_02", "sec_batch_key_03");
        String column = "batch_col";
        
        // Data distributed across partitions: p1(t1=50), p2(t2=150), p3(t3=250)
        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;


        for (String key : keys) {
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), column.getBytes(), t1, toBytes(key + "_v1"));
            hTable.put(put);

            Thread.sleep(5);
            put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), column.getBytes(), t2, toBytes(key + "_v2"));
            hTable.put(put);

            Thread.sleep(5);
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

        Result[] results = hTable.get(gets);
        Assert.assertEquals(3, results.length);
        for (int i = 0; i < results.length; i++) {
            Result r = results[i];
            Assert.assertEquals(1, r.rawCells().length);

            Assert.assertEquals(keys.get(i) + "_v3", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
            Assert.assertEquals(t3, r.rawCells()[0].getTimestamp());
        }
    }

    /**
     * Test Get optimization with time range on KEY-RANGE secondary partition
     * Verifies: Time range filtering correctly returns latest version within specified range across partitions
     */
    @Test
    public void testGetOptimizeWithTimeRangeOnKeyRange() throws Exception {
        // Initialize table - KEY-RANGE secondary partition
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_key_range");

        String family = "family1";
        String key = "sec_time_range_key";
        String column = "col";
        
        // Data distributed across partitions: p1(t1=50), p2(t2=150), p3(t3=250)
        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;


        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t1, toBytes("value_t1"));
        hTable.put(put);

        Thread.sleep(100);
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t2, toBytes("value_t2"));
        hTable.put(put);

        Thread.sleep(100);
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t3, toBytes("value_t3"));
        hTable.put(put);

        // Get with time range (t1 to t2+1)
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setTimeRange(t1, t2 + 1);
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        Assert.assertEquals("value_t2", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t2, r.rawCells()[0].getTimestamp());

        // Get with time range (t1 to t3+1)
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setTimeRange(t1, t3 + 1);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_t3", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t3, r.rawCells()[0].getTimestamp());
    }

    /**
     * Test Get optimization with specific timestamp on KEY-RANGE secondary partition
     * Verifies: Get with setTimestamp accurately retrieves data from correct partition, handles non-existent timestamp
     */
    @Test
    public void testGetOptimizeWithSpecificTimestampOnKeyRange() throws Exception {
        // Initialize table - KEY-RANGE secondary partition
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_key_range");

        String family = "family1";
        String key = "sec_specific_ts_key";
        String column = "col";
        
        // Data distributed across partitions: p1(t1=50), p2(t2=150), p3(t3=250)
        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;

        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t1, toBytes("value_t1"));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t2, toBytes("value_t2"));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t3, toBytes("value_t3"));
        hTable.put(put);

        // Get with specific timestamp t2
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setTimeStamp(t2);
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_t2", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t2, r.rawCells()[0].getTimestamp());

        // Get with specific timestamp t1
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setTimeStamp(t1);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_t1", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t1, r.rawCells()[0].getTimestamp());

        // Get with specific timestamp t3
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setTimeStamp(t3);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_t3", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t3, r.rawCells()[0].getTimestamp());

        // Get with non-existent timestamp (between t1 and t2)
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setTimeStamp(100L);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);
    }

    /**
     * Test Get optimization with specific timestamp on RANGE-KEY secondary partition
     * Verifies: Timestamp-based retrieval works correctly across different primary partitions
     */
    @Test
    public void testGetOptimizeWithSpecificTimestampOnRangeKey() throws Exception {
        // Initialize table - RANGE-KEY secondary partition
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_range_key");

        String family = "family1";
        String key = "sec_rk_specific_ts_key";
        String column = "col";
        
        // Data distributed: p1(t1=50, t4=80), p2(t2=150, t5=180), p3(t3=250)
        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;
        long t4 = 80L;
        long t5 = 180L;

        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t1, toBytes("value_t1"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t2, toBytes("value_t2"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t3, toBytes("value_t3"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t4, toBytes("value_t4"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column.getBytes(), t5, toBytes("value_t5"));
        hTable.put(put);

        // Get with timestamp t3 from p3
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setTimeStamp(t3);
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_t3", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t3, r.rawCells()[0].getTimestamp());

        // Get with timestamp t5 from p2
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setTimeStamp(t5);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_t5", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t5, r.rawCells()[0].getTimestamp());

        // Get with timestamp t4 from p1
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), column.getBytes());
        get.setTimeStamp(t4);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals("value_t4", Bytes.toString(r.rawCells()[0].getValueArray(), r.rawCells()[0].getValueOffset(), r.rawCells()[0].getValueLength()));
        Assert.assertEquals(t4, r.rawCells()[0].getTimestamp());
    }

    /**
     * Test Get optimization with multiple qualifiers on KEY-RANGE secondary partition
     * Verifies: Multiple qualifiers each return only latest version across partitions when MaxVersions defaults to 1
     */
    @Test
    public void testGetOptimizeWithMultipleQualifiersOnKeyRange() throws Exception {
        // Initialize table - KEY-RANGE secondary partition
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_key_range");

        String family = "family1";
        String key = "multi_qual_key_001";
        String col1 = "qualifier1";
        String col2 = "qualifier2";
        String col3 = "qualifier3";

        // Data distributed across partitions: p1(t1=50), p2(t2=150), p3(t3=250)
        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;

        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t1, toBytes("col1_v1"));
        put.addColumn(family.getBytes(), col2.getBytes(), t1, toBytes("col2_v1"));
        put.addColumn(family.getBytes(), col3.getBytes(), t1, toBytes("col3_v1"));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t2, toBytes("col1_v2"));
        put.addColumn(family.getBytes(), col2.getBytes(), t2, toBytes("col2_v2"));
        put.addColumn(family.getBytes(), col3.getBytes(), t2, toBytes("col3_v2"));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t3, toBytes("col1_v3"));
        put.addColumn(family.getBytes(), col2.getBytes(), t3, toBytes("col2_v3"));
        put.addColumn(family.getBytes(), col3.getBytes(), t3, toBytes("col3_v3"));
        hTable.put(put);

        // Get with multiple qualifiers + default MaxVersions=1 - should trigger optimization
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col1.getBytes());
        get.addColumn(family.getBytes(), col2.getBytes());
        get.addColumn(family.getBytes(), col3.getBytes());
        Result r = hTable.get(get);
        
        Assert.assertEquals(3, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[i].getValueOffset(), r.rawCells()[i].getValueLength());
            resultMap.put(qualifier, value);
        }
        
        Assert.assertEquals("col1_v3", resultMap.get(col1));
        Assert.assertEquals("col2_v3", resultMap.get(col2));
        Assert.assertEquals("col3_v3", resultMap.get(col3));
    }

    /**
     * Test Get optimization with multiple qualifiers on RANGE-KEY secondary partition
     * Verifies: Multiple qualifiers each return only latest version across all partitions
     */
    @Test
    public void testGetOptimizeWithMultipleQualifiersOnRangeKey() throws Exception {
        // Initialize table - RANGE-KEY secondary partition
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_range_key");

        String family = "family1";
        String key = "rk_multi_qual_key_001";
        String col1 = "q1";
        String col2 = "q2";
        String col3 = "q3";
        String col4 = "q4";

        // Data distributed: p1(t1=50, t4=80), p2(t2=150, t5=180), p3(t3=250)
        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;
        long t4 = 80L;
        long t5 = 180L;

        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t1, toBytes("q1_value_1"));
        put.addColumn(family.getBytes(), col2.getBytes(), t1, toBytes("q2_value_1"));
        put.addColumn(family.getBytes(), col3.getBytes(), t1, toBytes("q3_value_1"));
        put.addColumn(family.getBytes(), col4.getBytes(), t1, toBytes("q4_value_1"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t2, toBytes("q1_value_2"));
        put.addColumn(family.getBytes(), col2.getBytes(), t2, toBytes("q2_value_2"));
        put.addColumn(family.getBytes(), col3.getBytes(), t2, toBytes("q3_value_2"));
        put.addColumn(family.getBytes(), col4.getBytes(), t2, toBytes("q4_value_2"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t3, toBytes("q1_value_3"));
        put.addColumn(family.getBytes(), col2.getBytes(), t3, toBytes("q2_value_3"));
        put.addColumn(family.getBytes(), col3.getBytes(), t3, toBytes("q3_value_3"));
        put.addColumn(family.getBytes(), col4.getBytes(), t3, toBytes("q4_value_3"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t4, toBytes("q1_value_4"));
        put.addColumn(family.getBytes(), col2.getBytes(), t4, toBytes("q2_value_4"));
        put.addColumn(family.getBytes(), col3.getBytes(), t4, toBytes("q3_value_4"));
        put.addColumn(family.getBytes(), col4.getBytes(), t4, toBytes("q4_value_4"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t5, toBytes("q1_value_5"));
        put.addColumn(family.getBytes(), col2.getBytes(), t5, toBytes("q2_value_5"));
        put.addColumn(family.getBytes(), col3.getBytes(), t5, toBytes("q3_value_5"));
        put.addColumn(family.getBytes(), col4.getBytes(), t5, toBytes("q4_value_5"));
        hTable.put(put);

        // Get with multiple qualifiers + default MaxVersions=1 - should trigger optimization
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col1.getBytes());
        get.addColumn(family.getBytes(), col2.getBytes());
        get.addColumn(family.getBytes(), col3.getBytes());
        get.addColumn(family.getBytes(), col4.getBytes());
        Result r = hTable.get(get);
        
        Assert.assertEquals(4, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[i].getValueOffset(), r.rawCells()[i].getValueLength());
            resultMap.put(qualifier, value);
        }
        
        Assert.assertEquals("q1_value_3", resultMap.get(col1));
        Assert.assertEquals("q2_value_3", resultMap.get(col2));
        Assert.assertEquals("q3_value_3", resultMap.get(col3));
        Assert.assertEquals("q4_value_3", resultMap.get(col4));
    }

    /**
     * Test Get optimization with multiple qualifiers and specific timestamp on KEY-RANGE secondary partition
     * Verifies: All qualifiers return data at specified timestamp from correct partition
     */
    @Test
    public void testGetOptimizeWithMultipleQualifiersAndTimestamp() throws Exception {
        // Initialize table - KEY-RANGE secondary partition
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_key_range");

        String family = "family1";
        String key = "multi_qual_ts_key";
        String col1 = "qualifier_a";
        String col2 = "qualifier_b";
        String col3 = "qualifier_c";

        // Data distributed across partitions: p1(t1=50), p2(t2=150), p3(t3=250)
        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;

        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t1, toBytes("a_t1"));
        put.addColumn(family.getBytes(), col2.getBytes(), t1, toBytes("b_t1"));
        put.addColumn(family.getBytes(), col3.getBytes(), t1, toBytes("c_t1"));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t2, toBytes("a_t2"));
        put.addColumn(family.getBytes(), col2.getBytes(), t2, toBytes("b_t2"));
        put.addColumn(family.getBytes(), col3.getBytes(), t2, toBytes("c_t2"));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t3, toBytes("a_t3"));
        put.addColumn(family.getBytes(), col2.getBytes(), t3, toBytes("b_t3"));
        put.addColumn(family.getBytes(), col3.getBytes(), t3, toBytes("c_t3"));
        hTable.put(put);

        // Get with multiple qualifiers and specific timestamp t2
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col1.getBytes());
        get.addColumn(family.getBytes(), col2.getBytes());
        get.addColumn(family.getBytes(), col3.getBytes());
        get.setTimeStamp(t2);
        Result r = hTable.get(get);
        
        System.out.println(Arrays.toString(r.rawCells()));
        Assert.assertEquals(3, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[i].getValueOffset(), r.rawCells()[i].getValueLength());
            long timestamp = r.rawCells()[i].getTimestamp();
            resultMap.put(qualifier, value);
            Assert.assertEquals(t2, timestamp);
        }
        
        Assert.assertEquals("a_t2", resultMap.get(col1));
        Assert.assertEquals("b_t2", resultMap.get(col2));
        Assert.assertEquals("c_t2", resultMap.get(col3));
    }

    /**
     * Test Get optimization with multiple qualifiers across different partitions
     * Verifies: Get correctly retrieves latest version for each qualifier when they reside in different partitions
     */
    @Test
    public void testGetOptimizeWithMultipleQualifiersAcrossPartitions() throws Exception {
        // Initialize table - KEY-RANGE secondary partition
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_key_range");

        String family = "family1";
        String key = "multi_qual_cross_partition_key";
        String col1 = "qualifier_1";
        String col2 = "qualifier_2";
        String col3 = "qualifier_3";

        // Each qualifier's latest in different partition: col1→p3(t3=250), col2→p1(t4=70), col3→p2(t5=180)
        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;
        long t4 = 70L;
        long t5 = 180L;
        long t6 = 280L;
        
        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t1, toBytes("col1_p1"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t2, toBytes("col1_p2"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t3, toBytes("col1_p3"));
        hTable.put(put);
        
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col2.getBytes(), t1, toBytes("col2_p1_v1"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col2.getBytes(), t4, toBytes("col2_p1_v2"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col3.getBytes(), t1, toBytes("col3_p1"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col3.getBytes(), t5, toBytes("col3_p2"));
        hTable.put(put);

        // Get with multiple qualifiers
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), col1.getBytes());
        get.addColumn(family.getBytes(), col2.getBytes());
        get.addColumn(family.getBytes(), col3.getBytes());
        Result r = hTable.get(get);
        
        System.out.println(Arrays.toString(r.rawCells()));
        Assert.assertEquals(3, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        Map<String, Long> timestampMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[i].getValueOffset(), r.rawCells()[i].getValueLength());
            long timestamp = r.rawCells()[i].getTimestamp();
            resultMap.put(qualifier, value);
            timestampMap.put(qualifier, timestamp);
        }
        
        Assert.assertEquals("col1_p3", resultMap.get(col1));
        Assert.assertEquals(t3, (long) timestampMap.get(col1));
        
        Assert.assertEquals("col2_p1_v2", resultMap.get(col2));
        Assert.assertEquals(t4, (long) timestampMap.get(col2));
        
        Assert.assertEquals("col3_p2", resultMap.get(col3));
        Assert.assertEquals(t5, (long) timestampMap.get(col3));
    }

    /**
     * Test Get without specifying qualifiers on KEY-RANGE secondary partition
     * Verifies: Returns latest version of all qualifiers across partitions
     */
    @Test
    public void testGetOptimizeWithoutQualifierOnKeyRange() throws Exception {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_key_range");

        String family = "family1";
        String key = "no_qualifier_key_range_001";
        String col1 = "qualifier_1";
        String col2 = "qualifier_2";
        String col3 = "qualifier_3";

        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;

        for (int i = 1; i <= 3; i++) {
            long ts = (i == 1) ? t1 : (i == 2) ? t2 : t3;
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
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[i].getValueOffset(), r.rawCells()[i].getValueLength());
            resultMap.put(qualifier, value);
        }
        
        Assert.assertEquals("col1_value_3", resultMap.get(col1));
        Assert.assertEquals("col2_value_3", resultMap.get(col2));
        Assert.assertEquals("col3_value_3", resultMap.get(col3));
    }

    /**
     * Test Get without specifying qualifiers on RANGE-KEY secondary partition
     * Verifies: Returns latest version of all qualifiers across partitions
     */
    @Test
    public void testGetOptimizeWithoutQualifierOnRangeKey() throws Exception {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_range_key");

        String family = "family1";
        String key = "no_qualifier_range_key_001";
        String col1 = "q1";
        String col2 = "q2";
        String col3 = "q3";
        String col4 = "q4";

        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;
        long t4 = 80L;
        long t5 = 180L;

        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t1, toBytes("q1_v1"));
        put.addColumn(family.getBytes(), col2.getBytes(), t1, toBytes("q2_v1"));
        put.addColumn(family.getBytes(), col3.getBytes(), t1, toBytes("q3_v1"));
        put.addColumn(family.getBytes(), col4.getBytes(), t1, toBytes("q4_v1"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t2, toBytes("q1_v2"));
        put.addColumn(family.getBytes(), col2.getBytes(), t2, toBytes("q2_v2"));
        put.addColumn(family.getBytes(), col3.getBytes(), t2, toBytes("q3_v2"));
        put.addColumn(family.getBytes(), col4.getBytes(), t2, toBytes("q4_v2"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t3, toBytes("q1_v3"));
        put.addColumn(family.getBytes(), col2.getBytes(), t3, toBytes("q2_v3"));
        put.addColumn(family.getBytes(), col3.getBytes(), t3, toBytes("q3_v3"));
        put.addColumn(family.getBytes(), col4.getBytes(), t3, toBytes("q4_v3"));
        hTable.put(put);

        Get get = new Get(toBytes(key));
        get.addFamily(family.getBytes());
        Result r = hTable.get(get);
        
        Assert.assertEquals(4, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[i].getValueOffset(), r.rawCells()[i].getValueLength());
            resultMap.put(qualifier, value);
        }
        
        Assert.assertEquals("q1_v3", resultMap.get(col1));
        Assert.assertEquals("q2_v3", resultMap.get(col2));
        Assert.assertEquals("q3_v3", resultMap.get(col3));
        Assert.assertEquals("q4_v3", resultMap.get(col4));
    }

    /**
     * Test Get without specifying qualifiers with specific timestamp on KEY-RANGE
     * Verifies: All qualifiers return data at specified timestamp across partitions
     */
    @Test
    public void testGetOptimizeWithoutQualifierAndTimestampOnKeyRange() throws Exception {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_key_range");

        String family = "family1";
        String key = "no_qualifier_ts_key_range";
        String col1 = "qa";
        String col2 = "qb";
        String col3 = "qc";

        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;

        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t1, toBytes("qa_t1"));
        put.addColumn(family.getBytes(), col2.getBytes(), t1, toBytes("qb_t1"));
        put.addColumn(family.getBytes(), col3.getBytes(), t1, toBytes("qc_t1"));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t2, toBytes("qa_t2"));
        put.addColumn(family.getBytes(), col2.getBytes(), t2, toBytes("qb_t2"));
        put.addColumn(family.getBytes(), col3.getBytes(), t2, toBytes("qc_t2"));
        hTable.put(put);

        Thread.sleep(10);
        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t3, toBytes("qa_t3"));
        put.addColumn(family.getBytes(), col2.getBytes(), t3, toBytes("qb_t3"));
        put.addColumn(family.getBytes(), col3.getBytes(), t3, toBytes("qc_t3"));
        hTable.put(put);

        Get get = new Get(toBytes(key));
        get.addFamily(family.getBytes());
        get.setTimeStamp(t2);
        Result r = hTable.get(get);
        
        Assert.assertEquals(3, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[i].getValueOffset(), r.rawCells()[i].getValueLength());
            long timestamp = r.rawCells()[i].getTimestamp();
            resultMap.put(qualifier, value);
            Assert.assertEquals(t2, timestamp);
        }
        
        Assert.assertEquals("qa_t2", resultMap.get(col1));
        Assert.assertEquals("qb_t2", resultMap.get(col2));
        Assert.assertEquals("qc_t2", resultMap.get(col3));
    }

    /**
     * Test Get without specifying qualifiers with specific timestamp on RANGE-KEY
     * Verifies: All qualifiers return data at specified timestamp across different partitions
     */
    @Test
    public void testGetOptimizeWithoutQualifierAndTimestampOnRangeKey() throws Exception {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test_get_optimize_secondary_range_key");

        String family = "family1";
        String key = "no_qualifier_ts_range_key";
        String col1 = "qualifier_a";
        String col2 = "qualifier_b";
        String col3 = "qualifier_c";
        String col4 = "qualifier_d";

        long t1 = 50L;
        long t2 = 150L;
        long t3 = 250L;
        long t4 = 80L;
        long t5 = 180L;

        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t1, toBytes("a_v1"));
        put.addColumn(family.getBytes(), col2.getBytes(), t1, toBytes("b_v1"));
        put.addColumn(family.getBytes(), col3.getBytes(), t1, toBytes("c_v1"));
        put.addColumn(family.getBytes(), col4.getBytes(), t1, toBytes("d_v1"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t2, toBytes("a_v2"));
        put.addColumn(family.getBytes(), col2.getBytes(), t2, toBytes("b_v2"));
        put.addColumn(family.getBytes(), col3.getBytes(), t2, toBytes("c_v2"));
        put.addColumn(family.getBytes(), col4.getBytes(), t2, toBytes("d_v2"));
        hTable.put(put);

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), col1.getBytes(), t3, toBytes("a_v3"));
        put.addColumn(family.getBytes(), col2.getBytes(), t3, toBytes("b_v3"));
        put.addColumn(family.getBytes(), col3.getBytes(), t3, toBytes("c_v3"));
        put.addColumn(family.getBytes(), col4.getBytes(), t3, toBytes("d_v3"));
        hTable.put(put);

        Get get = new Get(toBytes(key));
        get.addFamily(family.getBytes());
        get.setTimeStamp(t2);
        Result r = hTable.get(get);
        
        Assert.assertEquals(4, r.rawCells().length);
        
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < r.rawCells().length; i++) {
            String qualifier = Bytes.toString(r.rawCells()[i].getQualifierArray(), r.rawCells()[i].getQualifierOffset(), r.rawCells()[i].getQualifierLength());
            String value = Bytes.toString(r.rawCells()[i].getValueArray(), r.rawCells()[i].getValueOffset(), r.rawCells()[i].getValueLength());
            long timestamp = r.rawCells()[i].getTimestamp();
            resultMap.put(qualifier, value);
            Assert.assertEquals(t2, timestamp);
        }
        
        Assert.assertEquals("a_v2", resultMap.get(col1));
        Assert.assertEquals("b_v2", resultMap.get(col2));
        Assert.assertEquals("c_v2", resultMap.get(col3));
        Assert.assertEquals("d_v2", resultMap.get(col4));

        // Verify another timestamp
        get = new Get(toBytes(key));
        get.addFamily(family.getBytes());
        get.setTimeStamp(t3);
        Result r2 = hTable.get(get);
        
        Assert.assertEquals(4, r2.rawCells().length);
        for (int i = 0; i < r2.rawCells().length; i++) {
            Assert.assertEquals(t3, r2.rawCells()[i].getTimestamp());
        }
    }
}
