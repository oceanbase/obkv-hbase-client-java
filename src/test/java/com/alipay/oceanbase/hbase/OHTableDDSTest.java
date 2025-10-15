/*-
 * #%L
 * OceanBase Table Hbase Framework
 * %%
 * Copyright (C) 2016 - 2021 Ant Financial Services Group
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PoolMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHTableDDSTest {
    private String        APP_NAME       = "obkv";
    private String        APP_DS_NAME_4x = "obkv4_adapt_dds_client_test";
    private String        APP_DS_NAME_2x = "obkv4_adapt_dds_client_test_2x";
    private String        VERSION        = "v1.0";
    private String        APP_DS_NAME    = APP_DS_NAME_2x;
    private String        hTableName     = APP_DS_NAME.equals(APP_DS_NAME_4x) ? "test" : "testt";

    protected OHTablePool ohTablePool;

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        ohTablePool = new OHTablePool(conf, 10, PoolMap.PoolType.Reusable);
        ohTablePool.setRuntimeBatchExecutor("testt", Executors.newFixedThreadPool(33));
        ohTablePool.init();
    }

    /*
     * 
     create table if not exists test$family_01(
        K varbinary(1024) NOT NULL,
        Q varbinary(256) NOT NULL,
        T bigint(20) NOT NULL,
        V varbinary(1024) NOT NULL,
        PRIMARY KEY (K, Q, T)
     ) TABLEGROUP = 'test';
    
     create table if not exists test$family_02(
        K varbinary(1024) NOT NULL,
        Q varbinary(256) NOT NULL,
        T bigint(20) NOT NULL,
        V varbinary(1024) NOT NULL,
        PRIMARY KEY (K, Q, T)
     ) TABLEGROUP = 'test';
     
     create table if not exists test$family_03(
        K varbinary(1024) NOT NULL,
        Q varbinary(256) NOT NULL,
        T bigint(20) NOT NULL,
        V varbinary(1024) NOT NULL,
        PRIMARY KEY (K, Q, T)
     ) TABLEGROUP = 'test';
      
     */

    /**
     * TestCase: 分表路由到单个分区，表为 test$family_00.
     * 扩展测试：覆盖00-09所有分区的单分区测试
     */
    @Test
    public void testSharding00() throws Exception {
        String family = "family";
        String column1 = "column1";
        String value = "value";
        long timestamp = System.currentTimeMillis();

        // 测试所有分区 00-09
        for (int partition = 0; partition <= 9; partition++) {
            String partitionPrefix = String.format("%02d", partition);
            String key1 = partitionPrefix + "_TESTKEY0";
            String key2 = partitionPrefix + "_TESTKEY1";
            String key3 = partitionPrefix + "_TESTKEY2";

            HTableInterface hTable = ohTablePool.getTable(hTableName);

            try {
                // 测试单个分区内的操作
                Put put = new Put(toBytes(key1));
                put.add(family.getBytes(), column1.getBytes(), timestamp,
                    toBytes(value + "_" + partitionPrefix));
                hTable.put(put);
                Get get = new Get(toBytes(key1));
                get.addColumn(family.getBytes(), toBytes(column1));
                Result r = hTable.get(get);
                Assert.assertEquals(1, r.raw().length);

                put = new Put(toBytes(key2));
                put.add(family.getBytes(), column1.getBytes(), timestamp,
                    toBytes(value + "_" + partitionPrefix));
                hTable.put(put);

                put = new Put(toBytes(key3));
                put.add(family.getBytes(), column1.getBytes(), timestamp,
                    toBytes(value + "_" + partitionPrefix));
                hTable.put(put);

                // 同分区内的scan应该成功
                Scan scan = new Scan();
                scan.addColumn(family.getBytes(), column1.getBytes());
                scan.setStartRow(toBytes(key1));
                scan.setStopRow(toBytes(key3));
                scan.setMaxVersions(9);
                ResultScanner scanner = hTable.getScanner(scan);

                int count = 0;
                for (Result result : scanner) {
                    for (KeyValue keyValue : result.raw()) {
                        System.out.println("Partition " + partitionPrefix + " - rowKey: "
                                           + new String(keyValue.getRow()) + " columnQualifier:"
                                           + new String(keyValue.getQualifier()) + " timestamp:"
                                           + keyValue.getTimestamp() + " value:"
                                           + new String(keyValue.getValue()));
                        count++;
                    }
                }
                scanner.close();
                Assert.assertEquals(2, count);

            } finally {
                // 清理数据
                Delete delete = new Delete(toBytes(key1));
                delete.deleteFamily(family.getBytes());
                hTable.delete(delete);
                delete = new Delete(toBytes(key2));
                delete.deleteFamily(family.getBytes());
                hTable.delete(delete);
                delete = new Delete(toBytes(key3));
                delete.deleteFamily(family.getBytes());
                hTable.delete(delete);
            }
        }
    }

    /**
     * TestCase: 分表路由到多个分区，分表为 test$family_{00-09}.
     * 扩展测试：覆盖所有00-09分区的跨分区测试
     */
    @Test
    public void testSharding01() throws Exception {
        String family = "family";
        String column1 = "column1";
        // 扩展到覆盖所有分区 00-09
        String key[] = new String[10];
        for (int i = 0; i < 10; i++) {
            key[i] = String.format("%02dTEST_KEY%d", i, i);
        }
        String value = "value";
        long timestamp = System.currentTimeMillis();

        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        HTableInterface hTable = ohTablePool.getTable(hTableName);

        try {
            // 测试所有分区的写入和读取
            for (int i = 0; i < key.length; i++) {
                Put put = new Put(toBytes(key[i]));
                put.add(family.getBytes(), column1.getBytes(), timestamp,
                    toBytes(value + "_partition_" + String.format("%02d", i)));
                hTable.put(put);
                Get get = new Get(toBytes(key[i]));
                get.addColumn(family.getBytes(), toBytes(column1));
                Result r = hTable.get(get);
                Assert.assertEquals(1, r.raw().length);
                System.out.println("Successfully wrote and read from partition "
                                   + String.format("%02d", i));
            }

            // 测试单个分区内的scan（应该成功）
            for (int partition = 0; partition < 10; partition++) {
                String partitionPrefix = String.format("%02d", partition);
                Scan scan = new Scan();
                scan.addColumn(family.getBytes(), column1.getBytes());
                scan.setStartRow(toBytes(partitionPrefix + "TEST_KEY" + partition));
                scan.setStopRow(toBytes(partitionPrefix + "TEST_KEY" + partition + "Z"));
                scan.setMaxVersions(9);
                ResultScanner scanner = hTable.getScanner(scan);

                int count = 0;
                for (Result result : scanner) {
                    for (KeyValue keyValue : result.raw()) {
                        System.out.println("Partition " + partitionPrefix + " scan - rowKey: "
                                           + new String(keyValue.getRow()) + " value: "
                                           + new String(keyValue.getValue()));
                        count++;
                    }
                }
                scanner.close();
                Assert.assertEquals(1, count);
            }

            // 跨 partition 的 Scan 是不支持的 - 测试多个跨分区场景
            int[][] crossPartitionTests = { { 0, 2 }, { 1, 4 }, { 5, 8 }, { 3, 9 } };
            for (int[] testCase : crossPartitionTests) {
                try {
                    Scan scan = new Scan();
                    scan.addColumn(family.getBytes(), column1.getBytes());
                    scan.setStartRow(toBytes(key[testCase[0]]));
                    scan.setStopRow(toBytes(key[testCase[1]]));
                    scan.setMaxVersions(9);
                    hTable.getScanner(scan);
                    Assert.fail("Expected exception for cross-partition scan from " + testCase[0]
                                + " to " + testCase[1]);
                } catch (Exception e) {
                    System.out.println("Cross-partition scan correctly failed: " + testCase[0]
                                       + " to " + testCase[1]);
                    Assert.assertTrue("Cross-partition scan should fail", true);
                }
            }
        } finally {
            for (int i = 0; i < key.length; i++) {
                Delete delete = new Delete(toBytes(key[i]));
                delete.deleteFamily(family.getBytes());
                hTable.delete(delete);
            }
            hTable.close();
        }
    }

    @Test
    public void testAppendIncrement() throws Exception {
        String family = "family";
        String column1 = "column1";
        String key1 = "00_TESTKEY0";
        String value = "value";
        String appendValue = "appendValue";
        String key2 = "00_TESTKEY1";
        long incrValue = 100L;

        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        HTableInterface hTable = ohTablePool.getTable(hTableName);

        try {
            long timestamp = System.currentTimeMillis();
            Put put = new Put(toBytes(key1));
            put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(value));
            hTable.put(put);

            Get get = new Get(toBytes(key1));
            get.addColumn(family.getBytes(), toBytes(column1));
            Result r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);

            Assert.assertEquals(value,
                new String(r.getValue(family.getBytes(), column1.getBytes())));

            Append append = new Append(toBytes(key1));
            append.add(family.getBytes(), column1.getBytes(), toBytes(appendValue));
            hTable.append(append);
            r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);
            Assert.assertEquals(value + appendValue,
                new String(r.getValue(family.getBytes(), column1.getBytes())));

            Increment increment = new Increment(toBytes(key2));
            increment.addColumn(family.getBytes(), column1.getBytes(), incrValue);
            hTable.increment(increment);
            get = new Get(toBytes(key2));
            get.addColumn(family.getBytes(), toBytes(column1));
            r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);
            System.out.println(Arrays.toString(r.getValue(family.getBytes(), column1.getBytes())));
            long actualValue = Bytes.toLong(r.getValue(family.getBytes(), column1.getBytes()));
            Assert.assertEquals(incrValue, actualValue);
        } finally {
            Delete delete = new Delete(toBytes(key1));
            delete.deleteFamily(family.getBytes());
            hTable.delete(delete);
            delete = new Delete(toBytes(key2));
            delete.deleteFamily(family.getBytes());
            hTable.delete(delete);
            hTable.close();
        }
    }

    @Test
    public void testCheckAndPut() throws Exception {
        String family = "family";
        String column1 = "column1";
        String key1 = "00_TESTKEY0";
        String value = "value";
        String newValue = "newValue";
        long timestamp = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        HTableInterface hTable = ohTablePool.getTable(hTableName);
        try {
            Put put = new Put(key1.getBytes());
            put.add(family.getBytes(), column1.getBytes(), value.getBytes());
            hTable.put(put);
            Get get = new Get(key1.getBytes());
            get.addColumn(family.getBytes(), column1.getBytes());
            Result r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);
            put = new Put(key1.getBytes());
            put.add(family.getBytes(), column1.getBytes(), newValue.getBytes());
            boolean ret = hTable.checkAndPut(key1.getBytes(), family.getBytes(),
                column1.getBytes(), value.getBytes(), put);
            Assert.assertTrue(ret);
            r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);
            Assert.assertEquals(newValue,
                new String(r.getValue(family.getBytes(), column1.getBytes())));
        } finally {
            Delete delete = new Delete(key1.getBytes());
            delete.deleteFamily(family.getBytes());
            hTable.delete(delete);
            hTable.close();
        }
    }

    /**
     * TestCase: 跨数据库测试 - 测试不同数据库 group_00-group_09 的表操作
     * 每个数据库包含 test$family_00-test$family_09 表，覆盖所有分区
     */
    @Test
    public void testCrossDatabase() throws Exception {
        String family = "family";
        String column1 = "column1";
        String value = "cross_db_value";

        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        HTableInterface hTable = ohTablePool.getTable(hTableName);

        List<String> testKeys = new ArrayList<>();
        try {
            // 测试跨数据库和跨表操作：使用key前缀路由到不同的数据库group_XX和表test$family_XX
            for (int dbIndex = 0; dbIndex <= 9; dbIndex++) {
                for (int tableIndex = 0; tableIndex <= 9; tableIndex++) {
                    // 使用特定格式的key确保路由到正确的数据库和表
                    // 格式：dbIndex_tableIndex_... 来路由到 group_dbIndex 数据库的 test$family_tableIndex 表
                    String key = String.format("%02d_%02d_CROSS_DB_TABLE_KEY", dbIndex, tableIndex);
                    testKeys.add(key);
                    
                    // 写入数据
                    Put put = new Put(toBytes(key));
                    put.add(family.getBytes(), column1.getBytes(), 
                        toBytes(value + "_db" + dbIndex + "_table" + tableIndex));
                    hTable.put(put);
                    
                    // 验证写入
                    Get get = new Get(toBytes(key));
                    get.addColumn(family.getBytes(), toBytes(column1));
                    Result r = hTable.get(get);
                    Assert.assertEquals(1, r.raw().length);
                    
                    String expectedValue = value + "_db" + dbIndex + "_table" + tableIndex;
                    String actualValue = new String(r.getValue(family.getBytes(), column1.getBytes()));
                    Assert.assertEquals(expectedValue, actualValue);
                    
                    System.out.println("Cross-DB-Table test: Successfully wrote/read group_" + 
                        String.format("%02d", dbIndex) + ".test$family_" + String.format("%02d", tableIndex) + 
                        " with key: " + key);
                }
            }
            
            // 测试同一分区内的批量操作（避免跨分区批量操作）
            // 只在一个特定的数据库表组合中进行批量操作
            int testDbIndex = 0;
            int testTableIndex = 0;
            List<Put> sameBatchPuts = new ArrayList<>();
            for (int i = 0; i < 5; i++) { // 在同一个分区中创建5条记录
                String batchKey = String.format("%02d_%02d_SAME_BATCH_KEY_%d", testDbIndex, testTableIndex, i);
                testKeys.add(batchKey);
                
                Put put = new Put(toBytes(batchKey));
                put.add(family.getBytes(), column1.getBytes(), 
                    toBytes("same_batch_value_" + i + "_db" + testDbIndex + "_table" + testTableIndex));
                sameBatchPuts.add(put);
            }
            
            // 执行同分区批量写入
            hTable.put(sameBatchPuts);
            System.out.println("Same-partition batch: Completed batch put of " + sameBatchPuts.size() + 
                " records in group_" + String.format("%02d", testDbIndex) + ".test$family_" + String.format("%02d", testTableIndex));
            
            // 验证批量写入的数据
            for (int i = 0; i < 5; i++) {
                String batchKey = String.format("%02d_%02d_SAME_BATCH_KEY_%d", testDbIndex, testTableIndex, i);
                Get get = new Get(toBytes(batchKey));
                get.addColumn(family.getBytes(), toBytes(column1));
                Result r = hTable.get(get);
                Assert.assertEquals(1, r.raw().length);
                
                String expectedValue = "same_batch_value_" + i + "_db" + testDbIndex + "_table" + testTableIndex;
                String actualValue = new String(r.getValue(family.getBytes(), column1.getBytes()));
                Assert.assertEquals(expectedValue, actualValue);
            }
            System.out.println("Same-partition batch verification completed successfully");
            
        } finally {
            // 清理数据
            for (String key : testKeys) {
                Delete delete = new Delete(toBytes(key));
                delete.deleteFamily(family.getBytes());
                hTable.delete(delete);
            }
            hTable.close();
        }
    }

    /**
     * TestCase: 跨表测试 - 测试同一数据库内不同表的操作
     * 在每个数据库 group_XX 中测试 test$family_00 到 test$family_09 的表操作
     */
    @Test
    public void testCrossTable() throws Exception {
        String family = "family";
        String column1 = "column1";
        String value = "cross_table_value";

        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        HTableInterface hTable = ohTablePool.getTable(hTableName);

        List<String> testKeys = new ArrayList<>();
        
        try {
            // 测试跨表操作：通过不同的key前缀路由到不同的表 test$family_XX
            // 这里只测试同一个数据库内的跨表操作
            for (int tableIndex = 0; tableIndex <= 9; tableIndex++) {
                for (int recordIndex = 0; recordIndex < 3; recordIndex++) { // 每个表3条记录
                    // 使用特定格式确保路由到正确的表
                    // 格式：00_tableIndex_... 来路由到 group_00 数据库的 test$family_tableIndex 表
                    String key = String.format("00_%02d_TABLE_KEY_%d", tableIndex, recordIndex);
                    testKeys.add(key);
                    
                    // 写入数据
                    Put put = new Put(toBytes(key));
                    put.add(family.getBytes(), column1.getBytes(), 
                        toBytes(value + "_table" + tableIndex + "_record" + recordIndex));
                    hTable.put(put);
                    
                    // 验证写入
                    Get get = new Get(toBytes(key));
                    get.addColumn(family.getBytes(), toBytes(column1));
                    Result r = hTable.get(get);
                    Assert.assertEquals(1, r.raw().length);
                    
                    String expectedValue = value + "_table" + tableIndex + "_record" + recordIndex;
                    String actualValue = new String(r.getValue(family.getBytes(), column1.getBytes()));
                    Assert.assertEquals(expectedValue, actualValue);
                    
                    System.out.println("Cross-table test: Successfully wrote/read group_00.test$family_" + 
                        String.format("%02d", tableIndex) + " record " + recordIndex + " with key: " + key);
                }
            }
            
            // 测试跨表的条件操作
            for (int tableIndex = 0; tableIndex < 5; tableIndex++) { // 测试前5个表
                String checkKey = String.format("00_%02d_CHECK_PUT", tableIndex);
                testKeys.add(checkKey);
                
                Put initialPut = new Put(toBytes(checkKey));
                initialPut.add(family.getBytes(), column1.getBytes(), toBytes("initial"));
                hTable.put(initialPut);
                
                Put updatePut = new Put(toBytes(checkKey));
                updatePut.add(family.getBytes(), column1.getBytes(), toBytes("updated"));
                boolean success = hTable.checkAndPut(toBytes(checkKey), family.getBytes(), 
                    column1.getBytes(), toBytes("initial"), updatePut);
                Assert.assertTrue("CheckAndPut should succeed for table " + tableIndex, success);
                
                Get get = new Get(toBytes(checkKey));
                get.addColumn(family.getBytes(), toBytes(column1));
                Result r = hTable.get(get);
                Assert.assertEquals("updated", 
                    new String(r.getValue(family.getBytes(), column1.getBytes())));
                
                System.out.println("Cross-table CheckAndPut: Successfully updated table " + tableIndex);
            }
            
        } finally {
            for (String key : testKeys) {
                try {
                    Delete delete = new Delete(toBytes(key));
                    delete.deleteFamily(family.getBytes());
                    hTable.delete(delete);
                } catch (Exception e) {
                }
            }
            hTable.close();
        }
    }

    /**
     * TestCase: 批量操作跨分区测试
     * 测试批量写入和读取跨多个分区的数据
     */
    @Test
    public void testBatchOperationsAcrossPartitions() throws Exception {
        String family = "family";
        String column1 = "column1";
        String value = "batch_value";
        long timestamp = System.currentTimeMillis();

        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        HTableInterface hTable = ohTablePool.getTable(hTableName);

        List<String> testKeys = new ArrayList<>();
        
        try {
            int totalSuccessCount = 0;
            for (int partition = 0; partition <= 9; partition++) {
                List<Put> partitionPuts = new ArrayList<>();
                List<Get> partitionGets = new ArrayList<>();
                List<String> partitionKeys = new ArrayList<>();
                
                for (int i = 0; i < 3; i++) { // 每个分区3条数据
                    String key = String.format("%02d_BATCH_KEY_%d_%d", partition, partition, i);
                    partitionKeys.add(key);
                    testKeys.add(key);
                    
                    Put put = new Put(toBytes(key));
                    put.add(family.getBytes(), column1.getBytes(), 
                        toBytes(value + "_" + partition + "_" + i));
                    partitionPuts.add(put);
                    
                    Get get = new Get(toBytes(key));
                    get.addColumn(family.getBytes(), toBytes(column1));
                    partitionGets.add(get);
                }
                
                hTable.put(partitionPuts);
                System.out.println("Batch put completed for partition " + String.format("%02d", partition) + 
                    ": " + partitionPuts.size() + " records");
                
                Result[] results = hTable.get(partitionGets);
                Assert.assertEquals(partitionGets.size(), results.length);
                
                int partitionSuccessCount = 0;
                for (int i = 0; i < results.length; i++) {
                    Result result = results[i];
                    if (result != null && result.raw().length > 0) {
                        String expectedValue = value + "_" + partition + "_" + i;
                        String actualValue = new String(result.getValue(family.getBytes(), column1.getBytes()));
                        Assert.assertEquals(expectedValue, actualValue);
                        partitionSuccessCount++;
                    }
                }
                Assert.assertEquals("All batch reads should succeed for partition " + partition, 
                    partitionKeys.size(), partitionSuccessCount);
                totalSuccessCount += partitionSuccessCount;
                
                System.out.println("Batch get completed for partition " + String.format("%02d", partition) + 
                    ": " + partitionSuccessCount + " records verified");
            }
            
            System.out.println("Total batch operations completed: " + totalSuccessCount + " records across all partitions");
            
        } finally {
            for (int partition = 0; partition <= 9; partition++) {
                List<Delete> partitionDeletes = new ArrayList<>();
                String partitionPrefix = String.format("%02d_", partition);
                
                for (String key : testKeys) {
                    if (key.startsWith(partitionPrefix)) {
                        Delete delete = new Delete(toBytes(key));
                        delete.deleteFamily(family.getBytes());
                        partitionDeletes.add(delete);
                    }
                }
                
                if (!partitionDeletes.isEmpty()) {
                    hTable.delete(partitionDeletes);
                    System.out.println("Cleaned up " + partitionDeletes.size() + " records from partition " + 
                        String.format("%02d", partition));
                }
            }
            hTable.close();
        }
    }

    @Test
    public void testCheckAndDelete() throws Exception {
        String family = "family";
        String column1 = "column1";
        String key1 = "00_TESTDELETEKEY";
        String key2 = "00_TESTDELETEKEY2";
        String value = "value";
        String newValue = "newValue";
        long timestamp = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        HTableInterface hTable = ohTablePool.getTable(hTableName);

        try {
            Put put = new Put(key1.getBytes());
            put.add(family.getBytes(), column1.getBytes(), value.getBytes());
            hTable.put(put);
            Get get = new Get(key1.getBytes());
            get.addColumn(family.getBytes(), column1.getBytes());
            Result r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);
            Delete delete = new Delete(key1.getBytes());
            delete.deleteColumn(family.getBytes(), column1.getBytes());
            boolean ret = hTable.checkAndDelete(key1.getBytes(), family.getBytes(),
                column1.getBytes(), value.getBytes(), delete);
            Assert.assertTrue(ret);
            r = hTable.get(get);
            Assert.assertEquals(0, r.raw().length);
        } finally {
            Delete delete = new Delete(key1.getBytes());
            delete.deleteFamily(family.getBytes());
            hTable.delete(delete);
            hTable.close();
        }
    }

    /*
        create tablegroup if not exists test_multicf SHARDING = 'ADAPTIVE';
        create table if not exists test_multicf$family1_00(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 
      
        create table if not exists test_multicf$family1_01(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 

        create table if not exists test_multicf$family1_02(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 

        create table if not exists test_multicf$family1_03(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 

        create table if not exists test_multicf$family2_00(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 

        create table if not exists test_multicf$family2_01(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 

        create table if not exists test_multicf$family2_02(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 

        create table if not exists test_multicf$family2_03(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 
     */

    /**
     * TestCase: 多CF功能测试, 每个CF进行分表
     */
    @Ignore
    public void testSharding02() throws Exception {
        String tableName = "test_multicf";
        String family1 = "family1";
        String family2 = "family2";
        String column1 = "column1";
        List<String> keyList = new ArrayList<>();
        for (int partition = 0; partition <= 9; partition++) {
            for (int i = 0; i < 2; i++) { // 每个分区2个key
                keyList.add(String.format("%02dTEST_KEY%d_%d", partition, partition, i));
            }
        }
        String[] key = keyList.toArray(new String[0]);
        String value = "value";

        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        HTableInterface hTable = new OHTable(conf, tableName);

        try {
            for (int i = 0; i < key.length; i++) {
                Put put = new Put(toBytes(key[i]));
                put.addColumn(family1.getBytes(), (column1 + family1).getBytes(), toBytes(value + key[i]
                        + family1));
                put.addColumn(family2.getBytes(), (column1 + family2).getBytes(), toBytes(value + key[i]
                        + family2));
                hTable.put(put);
                System.out.println("Multi-CF: Successfully wrote key " + key[i] + " to both families");
            }

            for (int i = 0; i < key.length; i++) {
                Get get = new Get(toBytes(key[i]));
                get.addFamily(family1.getBytes());
                get.addFamily(family2.getBytes());
                Result result = hTable.get(get);
                Assert.assertEquals(2, result.raw().length);
                System.out.println("Multi-CF: Successfully read key " + key[i] + " from both families");
            }

            List<Pair<byte[], byte[]>> scans = new ArrayList<>();
            for (int partition = 0; partition <= 9; partition++) {
                String startKey = String.format("%02dA", partition);
                String endKey = String.format("%02dZ", partition);
                scans.add(new Pair<>(toBytes(startKey), toBytes(endKey)));
            }
            for (int partition = 0; partition < scans.size(); partition++) {
                Pair<byte[], byte[]> scan = scans.get(partition);
                
                Scan s = new Scan(scan.getFirst(), scan.getSecond());
                s.addFamily(family1.getBytes());
                ResultScanner scanner = hTable.getScanner(s);
                int count1 = 0;
                for (Result result : scanner) {
                    System.out.println("Partition " + String.format("%02d", partition) + 
                        " Family1 Forward Scan result: " + Arrays.toString(result.raw()));
                    count1++;
                }
                scanner.close();
                
                s = new Scan(scan.getFirst(), scan.getSecond());
                s.addFamily(family2.getBytes());
                scanner = hTable.getScanner(s);
                int count2 = 0;
                for (Result result : scanner) {
                    System.out.println("Partition " + String.format("%02d", partition) + 
                        " Family2 Forward Scan result: " + Arrays.toString(result.raw()));
                    count2++;
                }
                scanner.close();
                
                Assert.assertEquals("Partition " + partition + " should have data in family1", count1, count2);
            }
            
            for (int partition = 0; partition < Math.min(5, scans.size()); partition++) {
                Pair<byte[], byte[]> scan = scans.get(partition);
                
                Scan s = new Scan(scan.getSecond(), scan.getFirst());
                s.addFamily(family1.getBytes());
                s.setReversed(true);
                ResultScanner scanner = hTable.getScanner(s);
                for (Result result : scanner) {
                    System.out.println("Partition " + String.format("%02d", partition) + 
                        " Family1 Reverse Scan result: " + Arrays.toString(result.raw()));
                }
                scanner.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("testSharding02 failed: " + e.getMessage());
        } finally {
            for (int i = 0; i < key.length; i++) {
                Delete delete = new Delete(toBytes(key[i]));
                delete.addFamily(family1.getBytes());
                delete.addFamily(family2.getBytes());
                hTable.delete(delete);
            }
        }

        hTable.close();
    }
}