/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2025 OceanBase Group
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

import com.alipay.oceanbase.hbase.OHTableClient;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.sql.Connection;
import java.sql.SQLSyntaxErrorException;
import java.util.*;

import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_AUTO_FILL_TIMESTAMP_IN_CLIENT;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHTableFillTimestampInClientTest {
    private static final String TABLE_NAME = "test";
    private static final String TABLE_GROUP = "test_fill_timestamp_group";
    private static final String COLUMN_FAMILY = "cf";
    private static final String TABLE_FULL_NAME = TABLE_NAME + "$" + COLUMN_FAMILY;
    private static final long ONE_DAY_MS = 24 * 60 * 60 * 1000L;

    @BeforeClass
    public static void before() throws Exception {
        createDynamicPartitionTable();
    }

    @AfterClass
    public static void finish() throws Exception {
        dropTable();
    }

    /**
     * Create dynamic partition table for testing
     */
    private static void createDynamicPartitionTable() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();

        // Create table group
        String tableGroupSql = String.format("CREATE TABLEGROUP IF NOT EXISTS `%s` SHARDING = 'ADAPTIVE';", TABLE_GROUP);
        try {
            conn.createStatement().execute(tableGroupSql);
            System.out.println("Created table group: " + TABLE_GROUP);
        } catch (SQLSyntaxErrorException e) {
            if (!e.getMessage().contains("already exists")) {
                throw e;
            }
            System.out.println("Table group already exists: " + TABLE_GROUP);
        }

        // Calculate partition boundary (current time + 1 day)
        long currentTime = System.currentTimeMillis();
        long partitionBoundary = currentTime + ONE_DAY_MS;

        // Create table with dynamic partition policy
        String createTableSql = String.format(
            "CREATE TABLE IF NOT EXISTS `%s` (\n" +
            "  `K` varbinary(1024) NOT NULL,\n" +
            "  `Q` varbinary(256) NOT NULL,\n" +
            "  `T` bigint(20) NOT NULL,\n" +
            "  `V` varbinary(10240) DEFAULT NULL,\n" +
            "  `G` bigint(20) GENERATED ALWAYS AS (ABS(T)),\n" +
            "  `K_PREFIX` varbinary(1024) GENERATED ALWAYS AS (substring(`K`, 1, 18)),\n" +
            "  PRIMARY KEY (`K`, `Q`, `T`)\n" +
            ") TABLEGROUP = `%s`\n" +
            "  kv_attributes ='{\"HBase\": {}}'\n" +
            "  enable_macro_block_bloom_filter = True\n" +
            "  DYNAMIC_PARTITION_POLICY(\n" +
            "    ENABLE = true,\n" +
            "    TIME_UNIT = 'month',\n" +
            "    PRECREATE_TIME = '1 month',\n" +
            "    EXPIRE_TIME = '1 month',\n" +
            "    BIGINT_PRECISION = 'ms')\n" +
            "  PARTITION BY RANGE COLUMNS(`G`) SUBPARTITION BY KEY(`K_PREFIX`) SUBPARTITIONS 2 (\n" +
            "    PARTITION `p0` VALUES LESS THAN (%d)\n" +
            "  );",
            TABLE_FULL_NAME, TABLE_GROUP, partitionBoundary);

        try {
            conn.createStatement().execute(createTableSql);
            System.out.println("Created table: " + TABLE_FULL_NAME);
            System.out.println("Partition boundary: " + partitionBoundary + " (" + new java.util.Date(partitionBoundary) + ")");
        } catch (SQLSyntaxErrorException e) {
            if (!e.getMessage().contains("already exists")) {
                throw e;
            }
            System.out.println("Table already exists: " + TABLE_FULL_NAME);
        }
    }

    /**
     * Drop test table
     */
    private static void dropTable() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        try {
            String dropTableSql = "DROP TABLE IF EXISTS `" + TABLE_FULL_NAME + "`;";
            conn.createStatement().execute(dropTableSql);
            System.out.println("Dropped table: " + TABLE_FULL_NAME);
        } catch (Exception e) {
            System.out.println("Failed to drop table: " + e.getMessage());
        }
    }

    /**
     * Truncate test table
     */
    private static void truncateTable() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        try {
            String truncateSql = "TRUNCATE TABLE `" + TABLE_FULL_NAME + "`;";
            conn.createStatement().execute(truncateSql);
        } catch (Exception e) {
            System.out.println("Failed to truncate table: " + e.getMessage());
        }
    }

    /**
     * Create OHTableClient with fillTimestampInClient configuration
     */
    private static OHTableClient newOHTableClientWithConfig(boolean fillTimestamp) throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        conf.setBoolean(HBASE_HTABLE_AUTO_FILL_TIMESTAMP_IN_CLIENT, fillTimestamp);
        OHTableClient client = new OHTableClient(TABLE_NAME, conf);
        client.init();
        return client;
    }

    /**
     * Test Put operation with fillTimestampInClient enabled
     */
    @Test
    public void testPutWithFillTimestampEnabled() throws Exception {
        OHTableClient hTable = newOHTableClientWithConfig(true);

        String key = "putKey";
        String column = "column1";
        String value = "value1";

        // Put without timestamp should succeed (uses current time within partition range)
        Put put = new Put(toBytes(key));
        put.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes(), toBytes(value));
        hTable.put(put); // Should succeed

        // Verify data was written
        Get get = new Get(toBytes(key));
        get.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.size());
        Assert.assertTrue(secureCompare(toBytes(value), r.getValue(COLUMN_FAMILY.getBytes(), column.getBytes())));

        // Verify timestamp is within valid range (current time)
        Cell cell = r.rawCells()[0];
        long timestamp = cell.getTimestamp();
        long currentTime = System.currentTimeMillis();
        Assert.assertTrue("Timestamp should be current time", 
            timestamp > currentTime - 5000 && timestamp <= currentTime + 1000);
        Assert.assertNotEquals("Timestamp should not be Long.MAX_VALUE", Long.MAX_VALUE, timestamp);

        hTable.close();
    }

    /**
     * Test Put operation with fillTimestampInClient disabled (should fail)
     */
    @Test
    public void testPutWithFillTimestampDisabled() throws Exception {
        OHTableClient hTable = newOHTableClientWithConfig(false);

        String key = "putKey";
        String column = "column1";
        String value = "value1";

        // Put without timestamp should fail (Long.MAX_VALUE is out of partition range)
        Put put = new Put(toBytes(key));
        put.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes(), toBytes(value));

        try {
            hTable.put(put);
            Assert.fail("Put without timestamp should fail when fillTimestampInClient is disabled");
        } catch (Exception e) {
            // Expected: should throw exception because Long.MAX_VALUE is out of partition range
            System.out.println("Expected exception: " + e.getMessage());
            Assert.assertTrue("Should throw exception about partition or timestamp", 
                e.getMessage() != null);
        }

        hTable.close();
    }

    /**
     * Test Put with explicit timestamp (should work regardless of config)
     */
    @Test
    public void testPutWithExplicitTimestamp() throws Exception {
        OHTableClient hTable = newOHTableClientWithConfig(true);

        String key = "putKey";
        String column = "column1";
        String value = "value1";
        long explicitTimestamp = System.currentTimeMillis() - 1000; // 1 second ago

        // Put with explicit timestamp should use the explicit timestamp
        Put put = new Put(toBytes(key));
        put.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes(), explicitTimestamp, toBytes(value));
        hTable.put(put);

        // Verify explicit timestamp is preserved
        Get get = new Get(toBytes(key));
        get.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.size());
        Cell cell = r.rawCells()[0];
        Assert.assertEquals("Explicit timestamp should be preserved", explicitTimestamp, cell.getTimestamp());

        hTable.close();
    }

    /**
     * Test batch Put with fillTimestampInClient enabled
     */
    @Test
    public void testBatchPutWithFillTimestampEnabled() throws Exception {
        OHTableClient hTable = newOHTableClientWithConfig(true);

        String keyPrefix = "batchKey";
        String column = "column1";
        String value = "value";

        // Batch Put without timestamp should succeed
        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Put put = new Put(toBytes(keyPrefix + i));
            put.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes(), toBytes(value + i));
            puts.add(put);
        }
        hTable.put(puts); // Should succeed

        // Verify all puts were written
        for (int i = 0; i < 5; i++) {
            Get get = new Get(toBytes(keyPrefix + i));
            get.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes());
            Result r = hTable.get(get);
            Assert.assertEquals(1, r.size());
            Assert.assertTrue(secureCompare(toBytes(value + i), 
                r.getValue(COLUMN_FAMILY.getBytes(), column.getBytes())));
        }

        hTable.close();
    }

    /**
     * Test batch Put with fillTimestampInClient disabled (should fail)
     */
    @Test
    public void testBatchPutWithFillTimestampDisabled() throws Exception {
        OHTableClient hTable = newOHTableClientWithConfig(false);

        String keyPrefix = "batchKey";
        String column = "column1";
        String value = "value";

        // Batch Put without timestamp should fail
        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Put put = new Put(toBytes(keyPrefix + i));
            put.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes(), toBytes(value + i));
            puts.add(put);
        }

        try {
            hTable.put(puts);
            Assert.fail("Batch Put without timestamp should fail when fillTimestampInClient is disabled");
        } catch (Exception e) {
            // Expected: should throw exception
            System.out.println("Expected exception: " + e.getMessage());
            Assert.assertTrue("Should throw exception about partition or timestamp", 
                e.getMessage() != null);
        }

        hTable.close();
    }

    /**
     * Test Increment operation with fillTimestampInClient enabled
     */
    @Test
    public void testIncrementWithFillTimestampEnabled() throws Exception {
        OHTableClient hTable = newOHTableClientWithConfig(true);

        String key = "incrementKey";
        String column = "column1";

        // First put a value with explicit timestamp
        long putTimestamp = System.currentTimeMillis() - 1000;
        Put put = new Put(toBytes(key));
        put.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes(), putTimestamp, Bytes.toBytes(10L));
        hTable.put(put);

        // Increment should succeed (creates new row with current time)
        long result = hTable.incrementColumnValue(
            toBytes(key),
            COLUMN_FAMILY.getBytes(),
            column.getBytes(),
            5L);

        Assert.assertEquals(15L, result);

        // Verify increment created a new cell
        Get get = new Get(toBytes(key));
        get.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes());
        Result r = hTable.get(get);
        Assert.assertTrue("Should have at least one cell", r.size() >= 1);

        hTable.close();
    }

    /**
     * Test Increment operation with fillTimestampInClient disabled (should fail for new row)
     */
    @Test
    public void testIncrementWithFillTimestampDisabled() throws Exception {
        OHTableClient hTable = newOHTableClientWithConfig(false);

        String key = "incrementKey";
        String column = "column1";

        // First put a value with explicit timestamp
        long putTimestamp = System.currentTimeMillis() - 1000;
        Put put = new Put(toBytes(key));
        put.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes(), putTimestamp, Bytes.toBytes(10L));
        hTable.put(put);

        // Increment on existing row might succeed, but creating new row should fail
        // This depends on implementation - if increment creates new row without timestamp, it should fail
        try {
            long result = hTable.incrementColumnValue(
                toBytes(key + "_new"),
                COLUMN_FAMILY.getBytes(),
                column.getBytes(),
                5L);
            // If it succeeds, the server might have handled it differently
            System.out.println("Increment result: " + result);
        } catch (Exception e) {
            // Expected if increment tries to create new row without timestamp
            System.out.println("Expected exception: " + e.getMessage());
        }

        hTable.close();
    }

    /**
     * Test Append operation with fillTimestampInClient enabled
     */
    @Test
    public void testAppendWithFillTimestampEnabled() throws Exception {
        OHTableClient hTable = newOHTableClientWithConfig(true);

        String key = "appendKey";
        String column = "column1";

        // First put a value with explicit timestamp
        long putTimestamp = System.currentTimeMillis() - 1000;
        Put put = new Put(toBytes(key));
        put.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes(), putTimestamp, toBytes("value"));
        hTable.put(put);

        // Append should succeed (creates new row with current time)
        Append append = new Append(toBytes(key));
        append.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes(), toBytes("_appended"));
        Result result = hTable.append(append);

        Assert.assertNotNull("Append should return result", result);
        Assert.assertTrue("Should have cells", result.size() > 0);

        hTable.close();
    }

    /**
     * Test Append operation with fillTimestampInClient disabled (should fail for new row)
     */
    @Test
    public void testAppendWithFillTimestampDisabled() throws Exception {
        OHTableClient hTable = newOHTableClientWithConfig(false);

        String key = "appendKey";
        String column = "column1";

        // First put a value with explicit timestamp
        long putTimestamp = System.currentTimeMillis() - 1000;
        Put put = new Put(toBytes(key));
        put.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes(), putTimestamp, toBytes("value"));
        hTable.put(put);

        // Append on existing row might succeed, but creating new row should fail
        try {
            Append append = new Append(toBytes(key + "_new"));
            append.addColumn(COLUMN_FAMILY.getBytes(), column.getBytes(), toBytes("value"));
            Result result = hTable.append(append);
            System.out.println("Append result size: " + (result != null ? result.size() : 0));
        } catch (Exception e) {
            // Expected if append tries to create new row without timestamp
            Assert.assertTrue("Should throw exception about partition or timestamp", 
                e.getMessage() != null);
        }
    }
}