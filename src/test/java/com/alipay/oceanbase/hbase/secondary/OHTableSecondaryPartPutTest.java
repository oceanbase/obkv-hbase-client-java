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
import com.alipay.oceanbase.hbase.util.TableTemplateManager;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.*;

import java.util.*;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHTableSecondaryPartPutTest {
    private static List<String>              tableNames       = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = new LinkedHashMap<>();


    @BeforeClass
    public static void before() throws Exception {
        openDistributedExecute();
        for (TableTemplateManager.TableType type : TableTemplateManager.NORMAL_TABLES) {
            createTables(type, tableNames, group2tableNames, true);
        }
    }

    @AfterClass
    public static void finish() throws Exception {
        closeDistributedExecute();
        dropTables(tableNames, group2tableNames);
    }

    @Before
    public void prepareCase() throws Exception {
        truncateTables(tableNames, group2tableNames);
    }

    public static void testPutImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        String value = "value";

        { // put new key and get
            Put put = new Put(toBytes(key));
            put.add(family.getBytes(), column1.getBytes(), toBytes(column1 + value));
            put.add(family.getBytes(), column2.getBytes(), toBytes(column2 + value));
            hTable.put(put);
            
            Get get = new Get(toBytes(key));
            get.addColumn(family.getBytes(), column1.getBytes());
            get.addColumn(family.getBytes(), column2.getBytes());
            Result r = hTable.get(get);
            Assert(tableName, ()->Assert.assertEquals(2, r.size()));
            Assert(tableName, ()->Assert.assertTrue(ObHTableTestUtil.secureCompare((column1 + value).getBytes(), r.getValue(family.getBytes(), column1.getBytes()))));
            Assert(tableName, ()->Assert.assertTrue(ObHTableTestUtil.secureCompare((column1 + value).getBytes(), r.getValue(family.getBytes(), column1.getBytes()))));
        }
        { // put exist key and get
            long timestamp = System.currentTimeMillis();
            Put put = new Put(toBytes(key));
            put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value + timestamp));
            hTable.put(put);
            
            Get get = new Get(toBytes(key));
            get.addColumn(family.getBytes(), column1.getBytes());
            Result r = hTable.get(get);
            Assert(tableName, ()->Assert.assertEquals(1, r.size()));
            Assert(tableName, ()->Assert.assertTrue(ObHTableTestUtil.secureCompare(toBytes(column1 + value + timestamp), r.getValue(family.getBytes(), column1.getBytes()))));
        }

        { // test timestamp update
            long timestamp = System.currentTimeMillis();
            Put put = new Put(toBytes(key));
            put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value + timestamp));
            hTable.put(put);
            
            Get get = new Get(toBytes(key));
            get.addColumn(family.getBytes(), column1.getBytes());
            Result r = hTable.get(get);
            Assert(tableName, ()->Assert.assertEquals(1, r.size()));
            Assert(tableName, ()->Assert.assertTrue(ObHTableTestUtil.secureCompare(toBytes(column1 + value + timestamp), r.getValue(family.getBytes(), column1.getBytes()))));
            Assert(tableName, ()->Assert.assertEquals(timestamp, r.rawCells()[0].getTimestamp()));
            
            Put put1 = new Put(toBytes(key));
            put1.add(family.getBytes(), column1.getBytes(), timestamp + 100, toBytes(column1 + value));
            hTable.put(put1);
            
            Result r2 = hTable.get(get);
            Assert(tableName, ()->Assert.assertEquals(1, r2.size()));
            Assert(tableName, ()->Assert.assertTrue(ObHTableTestUtil.secureCompare(toBytes(column1 + value), r2.getValue(family.getBytes(), column1.getBytes()))));
            Assert(tableName, ()->Assert.assertTrue(timestamp < r2.rawCells()[0].getTimestamp()));
        }

       
        
        hTable.close();
    }
    
    public static void testBatchPutImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        String value = "value";
        {
            long timestamp = System.currentTimeMillis();
            List<Put> puts = new ArrayList<>();
            Get get = new Get(toBytes(key));
            for (int i = 0; i < 10; ++i) {
                Put put = new Put(toBytes(key));
                put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value + timestamp + i));
                put.add(family.getBytes(), column2.getBytes(), timestamp, toBytes(column2 + value + timestamp + i));
                puts.add(put);
                get.addColumn(family.getBytes(), column1.getBytes());
                get.addColumn(family.getBytes(), column2.getBytes());
            }

            hTable.batch(puts);
            Result result = hTable.get(get);
            Assert(tableName, ()->Assert.assertEquals(2, result.size()));
            for (Cell cell : result.rawCells()) {
                Assert(tableName, ()->Assert.assertEquals(timestamp, cell.getTimestamp()));
            }
            Assert(tableName, () -> Assert.assertTrue(secureCompare((column1 + value + timestamp + 9).getBytes(), result.getValue(family.getBytes(), column1.getBytes()))));
        }
        {
            long timestamp = System.currentTimeMillis();
            List<Put> puts = new ArrayList<>();
            List<Get> gets = new ArrayList<>();
            
            for (int i = 0; i < 10; ++i) {
                Put put = new Put(toBytes(key + i));
                put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value + timestamp + i));
                put.add(family.getBytes(), column2.getBytes(), timestamp, toBytes(column2 + value + timestamp + i));
                puts.add(put);
                Get get = new Get(toBytes(key + i));
                get.addColumn(family.getBytes(), column1.getBytes());
                get.addColumn(family.getBytes(), column2.getBytes());
                gets.add(get);
            }
            
            hTable.batch(puts);
            
            for (int i = 0; i < 10; ++i) {
                Result result = hTable.get(gets.get(i));
                Assert(tableName, ()->Assert.assertEquals(2, result.size()));
                for (Cell cell : result.rawCells()) {
                    Assert(tableName, ()->Assert.assertEquals(timestamp, cell.getTimestamp()));
                }
                int finalI = i;
                Assert(tableName, () -> Assert.assertTrue(secureCompare((column1 + value + timestamp + finalI).getBytes(), result.getValue(family.getBytes(), column1.getBytes()))));
            }
        }
        
        
        hTable.close();
    }

    public static void testMultiCFPutImpl(Map.Entry<String, List<String>> entry) throws Exception {
        String key = "putKey";
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        String value = "value";
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(entry.getKey()));
        hTable.init();
        {
            long currentTime = System.currentTimeMillis();
            Put put = new Put(toBytes(key));
            Get get = new Get(toBytes(key));
            for (String tableName : entry.getValue()) {
                String family = getColumnFamilyName(tableName);
                put.add(family.getBytes(), column1.getBytes(), toBytes(column1 + value));
                put.add(family.getBytes(), column2.getBytes(), toBytes(column2 + value));
                get.addColumn(family.getBytes(), column1.getBytes());
                get.addColumn(family.getBytes(), column2.getBytes());
            }
            hTable.put(put);
            Result r = hTable.get(get);
            Assert(entry.getValue(), ()->Assert.assertEquals(entry.getValue().size() * 2, r.size()));
            long remoteTimestamp = r.rawCells()[0].getTimestamp();
            Assert(entry.getValue(), ()->Assert.assertTrue(remoteTimestamp >= currentTime));
            for (Cell cell : r.rawCells()) {
                Assert(entry.getValue(), ()->Assert.assertEquals(remoteTimestamp, cell.getTimestamp()));
            }
        }

        {
            long timestamp = System.currentTimeMillis();
            Put put = new Put(toBytes(key));
            Get get = new Get(toBytes(key));
            for (String tableName : entry.getValue()) {
                String family = getColumnFamilyName(tableName);
                put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value));
                put.add(family.getBytes(), column2.getBytes(), timestamp, toBytes(column2 + value));
                get.addColumn(family.getBytes(), column1.getBytes());
                get.addColumn(family.getBytes(), column2.getBytes());
            }
            
            hTable.put(put);
            Result r = hTable.get(get);
            Assert(entry.getValue(), ()->Assert.assertEquals(entry.getValue().size() * 2, r.size()));
            for (Cell cell : r.rawCells()) {
                Assert(entry.getValue(), ()->Assert.assertEquals(timestamp, cell.getTimestamp()));
            }
            for (String tableName : entry.getValue()) {
                String family = getColumnFamilyName(tableName);
                // TODO: Get/Scan返回的结果Q 带了cf, 这里预期跑不过
                Assert(entry.getValue(), () -> Assert.assertTrue(secureCompare((column1 + value).getBytes(), r.getValue(family.getBytes(), column1.getBytes()))));
                Assert(entry.getValue(), () -> Assert.assertTrue(secureCompare((column2 + value).getBytes(), r.getValue(family.getBytes(), column2.getBytes()))));
            }
        }
        
        hTable.close();
    }

    public static void testMltiCFPutBatchImpl(Map.Entry<String, List<String>> entry) throws Exception {
        String key = "putKey";
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        String value = "value";
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(entry.getKey()));
        hTable.init();

        {
            long timestamp = System.currentTimeMillis();
            List<Put> puts = new ArrayList<>();
            Get get = new Get(toBytes(key));
            for (String tableName : entry.getValue()) {
                String family = getColumnFamilyName(tableName);
                Put put = new Put(toBytes(key));
                put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value + timestamp));
                put.add(family.getBytes(), column2.getBytes(), timestamp, toBytes(column2 + value + timestamp));
                puts.add(put);
                get.addColumn(family.getBytes(), column1.getBytes());
                get.addColumn(family.getBytes(), column2.getBytes());
            }
            hTable.batch(puts);
            Result result = hTable.get(get);
            Assert(entry.getValue(), ()->Assert.assertEquals(entry.getValue().size() * 2, result.size()));
            for (Cell cell : result.rawCells()) {
                Assert(entry.getValue(), ()->Assert.assertEquals(timestamp, cell.getTimestamp()));
            }
            for (String tableName : entry.getValue()) {
                String family = getColumnFamilyName(tableName);
                // TODO: Get/Scan返回的结果Q 带了cf, 这里预期跑不过
                Assert(entry.getValue(), () -> Assert.assertTrue(secureCompare((column1 + value + timestamp).getBytes(), result.getValue(family.getBytes(), column1.getBytes()))));
                Assert(entry.getValue(), () -> Assert.assertTrue(secureCompare((column2 + value + timestamp).getBytes(), result.getValue(family.getBytes(), column2.getBytes()))));
            }
        }

        {
            long timestamp = System.currentTimeMillis();
            List<Put> puts = new ArrayList<>();
            List<Pair<Get,String>> gets = new ArrayList<>();
            
            for (String tableName : entry.getValue()) {
                String family = getColumnFamilyName(tableName);
                for (int i = 0; i < 10; ++i) {
                    Put put = new Put(toBytes(key + i));
                    put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value + timestamp));
                    put.add(family.getBytes(), column2.getBytes(), timestamp, toBytes(column2 + value + timestamp));
                    puts.add(put);
                    Get get = new Get(toBytes(key + i));
                    get.addColumn(family.getBytes(), column1.getBytes());
                    get.addColumn(family.getBytes(), column2.getBytes());
                    gets.add(new Pair<>(get, family));
                }
            }
            hTable.batch(puts);
            for (int i = 0; i < 10; ++i) {
                Result result = hTable.get(gets.get(i).getFirst());
                Assert(entry.getValue(), () -> Assert.assertEquals(2, result.size()));
                for (Cell cell : result.rawCells()) {
                    Assert(entry.getValue(), () -> Assert.assertEquals(timestamp, cell.getTimestamp()));
                }
                int finalI = i;
                Assert(entry.getValue(), () -> Assert.assertTrue(secureCompare((column1 + value + timestamp).getBytes(), result.getValue(gets.get(finalI).getSecond().getBytes(), column1.getBytes()))));
                Assert(entry.getValue(), () -> Assert.assertTrue(secureCompare((column2 + value + timestamp).getBytes(), result.getValue(gets.get(finalI).getSecond().getBytes(), column2.getBytes()))));
            }
            
        }
        hTable.close();
    }
    
    
    @Test
    public void testPut() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartPutTest::testPutImpl);
    }
    
    @Test
    public void testBatchPut() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartPutTest::testBatchPutImpl);
    }
    
    @Test
    public void testMultiCFPut() throws Throwable {
        FOR_EACH(group2tableNames, OHTableSecondaryPartPutTest::testMultiCFPutImpl);
    }
    
    @Test
    public void testMultiCFPutBatch() throws Throwable {
        FOR_EACH(group2tableNames, OHTableSecondaryPartPutTest::testMltiCFPutBatchImpl);
    }

}
