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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.*;

import java.util.*;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHTableSecondaryPartPutTest {
    private static List<String>              tableNames       = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = new LinkedHashMap<String, List<String>>();

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
            long timestamp = System.currentTimeMillis();
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value));
            put.addColumn(family.getBytes(), column2.getBytes(), timestamp, toBytes(column2 + value));
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
            put.addColumn(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value + timestamp));
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
            put.addColumn(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value + timestamp));
            hTable.put(put);
            
            Get get = new Get(toBytes(key));
            get.addColumn(family.getBytes(), column1.getBytes());
            Result r = hTable.get(get);
            Assert(tableName, ()->Assert.assertEquals(1, r.size()));
            Assert(tableName, ()->Assert.assertTrue(ObHTableTestUtil.secureCompare(toBytes(column1 + value + timestamp), r.getValue(family.getBytes(), column1.getBytes()))));
            Assert(tableName, ()->Assert.assertEquals(timestamp, r.rawCells()[0].getTimestamp()));
            
            Put put1 = new Put(toBytes(key));
            put1.addColumn(family.getBytes(), column1.getBytes(), timestamp + 100, toBytes(column1 + value));
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
                put.addColumn(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value + timestamp + i));
                put.addColumn(family.getBytes(), column2.getBytes(), timestamp, toBytes(column2 + value + timestamp + i));
                puts.add(put);
                get.addColumn(family.getBytes(), column1.getBytes());
                get.addColumn(family.getBytes(), column2.getBytes());
            }
            Result[] results = new Result[puts.size()];
            hTable.batch(puts, results);
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
                put.addColumn(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value + timestamp + i));
                put.addColumn(family.getBytes(), column2.getBytes(), timestamp, toBytes(column2 + value + timestamp + i));
                puts.add(put);
                Get get = new Get(toBytes(key + i));
                get.addColumn(family.getBytes(), column1.getBytes());
                get.addColumn(family.getBytes(), column2.getBytes());
                gets.add(get);
            }

            Result[] results = new Result[puts.size()];
            hTable.batch(puts, results);
            
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
                put.addColumn(family.getBytes(), column1.getBytes(), currentTime, toBytes(column1 + value));
                put.addColumn(family.getBytes(), column2.getBytes(), currentTime, toBytes(column2 + value));
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
                put.addColumn(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value));
                put.addColumn(family.getBytes(), column2.getBytes(), timestamp, toBytes(column2 + value));
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
                put.addColumn(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value + timestamp));
                put.addColumn(family.getBytes(), column2.getBytes(), timestamp, toBytes(column2 + value + timestamp));
                puts.add(put);
                get.addColumn(family.getBytes(), column1.getBytes());
                get.addColumn(family.getBytes(), column2.getBytes());
            }
            Result[] results = new Result[puts.size()];
            hTable.batch(puts, results);
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
                    put.addColumn(family.getBytes(), column1.getBytes(), timestamp, toBytes(column1 + value + timestamp));
                    put.addColumn(family.getBytes(), column2.getBytes(), timestamp, toBytes(column2 + value + timestamp));
                    puts.add(put);
                    Get get = new Get(toBytes(key + i));
                    get.addColumn(family.getBytes(), column1.getBytes());
                    get.addColumn(family.getBytes(), column2.getBytes());
                    gets.add(new Pair<>(get, family));
                }
            }
            Result[] results = new Result[puts.size()];
            hTable.batch(puts, results);
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

    @Test
    public void testPutOpt() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartPutTest::testPutOptImpl);
        FOR_EACH(group2tableNames, OHTableSecondaryPartPutTest::testMultiCFPutOptImpl);
    }

    public static void testPutOptImpl(String tableName) throws Exception {
        int NUM_QUALIFIERS = 10;
        int NUM_PUTS = 10;
        byte[] family = toBytes(getColumnFamilyName(tableName));
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        {   // 单put，10个Qualifier
            Put put = new Put(toBytes("row1"));
            List<String> qualifiers = new ArrayList<>();
            for (int i = 1; i <= NUM_QUALIFIERS; i++) {
                byte[] qualifier = toBytes("q" + i);
                byte[] value = toBytes("value" + i);
                put.addColumn(family, qualifier, value);
                qualifiers.add(Bytes.toString(qualifier));
            }
            hTable.put(put);
            // verify
            Get get = new Get(Bytes.toBytes("row1"));
            Result result = hTable.get(get);
            Assert.assertEquals(NUM_QUALIFIERS, result.getFamilyMap(family).size());
            qualifiers.forEach(q ->
                    Assert.assertNotNull("Qualifier " + q + " not found",
                            result.getValue(family, Bytes.toBytes(q)))
            );
        }

        {   // 单put，10个Qualifier，指定相同timestamp
            Put put = new Put(toBytes("row2"));
            List<String> qualifiers = new ArrayList<>();
            long timestamp = System.currentTimeMillis();
            for (int i = 1; i <= NUM_QUALIFIERS; i++) {
                byte[] qualifier = toBytes("q" + i);
                byte[] value = toBytes("value" + i);
                put.addColumn(family, qualifier, timestamp, value);
                qualifiers.add(Bytes.toString(qualifier));
            }
            hTable.put(put);
            // verify
            Get get = new Get(Bytes.toBytes("row2"));
            Result result = hTable.get(get);
            Assert.assertEquals(NUM_QUALIFIERS, result.getFamilyMap(family).size());
            qualifiers.forEach(q ->
                    Assert.assertNotNull("Qualifier " + q + " not found",
                            result.getValue(family, Bytes.toBytes(q)))
            );
        }
        {   // batch put，相同key， 多个Qualifier
            byte[] rowKey = Bytes.toBytes("batch_row");
            List<Put> puts = new ArrayList<>();
            List<String> qualifiers = new ArrayList<>();
            for (int i = 1; i <= NUM_PUTS; i++) {
                Put put = new Put(rowKey);
                byte[] qualifier = Bytes.toBytes("batch_q" + i);
                put.addColumn(family, qualifier, Bytes.toBytes("batch_val" + i));
                puts.add(put);
                qualifiers.add(Bytes.toString(qualifier));
            }
            hTable.put(puts);
            // verify
            Get get = new Get(rowKey);
            Result result = hTable.get(get);
            Assert.assertEquals(NUM_QUALIFIERS, result.getFamilyMap(family).size());
            qualifiers.forEach(q ->
                    Assert.assertNotNull("Qualifier " + q + " not found",
                            result.getValue(family, Bytes.toBytes(q)))
            );
        }

        {   // batch put，相同key，多个Qualifier，指定相同timestamp
            byte[] rowKey = Bytes.toBytes("batch_row_ts");
            List<Put> puts = new ArrayList<>();
            List<String> qualifiers = new ArrayList<>();
            for (int i = 1; i <= NUM_PUTS; i++) {
                Put put = new Put(rowKey);
                byte[] qualifier = Bytes.toBytes("batch_ts_q" + i);
                put.addColumn(family, qualifier, Bytes.toBytes("batch_val" + i));
                puts.add(put);
                qualifiers.add(Bytes.toString(qualifier));
            }
            hTable.put(puts);
            // verify
            Get get = new Get(rowKey);
            Result result = hTable.get(get);
            Assert.assertEquals(NUM_QUALIFIERS, result.getFamilyMap(family).size());
            qualifiers.forEach(q ->
                    Assert.assertNotNull("Qualifier " + q + " not found",
                            result.getValue(family, Bytes.toBytes(q)))
            );
        }
    }

    public static void testMultiCFPutOptImpl(Map.Entry<String, List<String>> entry) throws Exception {
        int NUM_QUALIFIERS = 10;
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(entry.getKey()));
        hTable.init();
        {
            Put put = new Put(toBytes("multi_cf_row"));
            for (String tableName : entry.getValue()) {
                byte[] family = toBytes(getColumnFamilyName(tableName));
                // 单put，10个Qualifier
                List<String> qualifiers = new ArrayList<>();
                for (int i = 0; i < NUM_QUALIFIERS; i++) {
                    byte[] qualifier = toBytes("q" + i);
                    byte[] value = toBytes("value_" + i);
                    put.addColumn(family, qualifier, value);
                    qualifiers.add(Bytes.toString(qualifier));
                }
            }
            hTable.put(put);
            // verify
            Get get = new Get(Bytes.toBytes("multi_cf_row"));
            Result result = hTable.get(get);
            for (String tableName : entry.getValue()) {
                byte[] family = toBytes(getColumnFamilyName(tableName));
                Assert.assertEquals(NUM_QUALIFIERS,
                        result.getFamilyMap(family).size());

                for (int i = 0; i < NUM_QUALIFIERS; i++) {
                    byte[] val = result.getValue(family, Bytes.toBytes("q" + i));
                    Assert.assertEquals( "value_" + i,
                            Bytes.toString(val));
                }
            }
        }

        {   // 指定timestamp
            long currentTimestamp = System.currentTimeMillis();
            Put put = new Put(toBytes("multi_cf_ts_row"));
            for (String tableName : entry.getValue()) {
                byte[] family = toBytes(getColumnFamilyName(tableName));
                // 单put，10个Qualifier
                List<String> qualifiers = new ArrayList<>();
                for (int i = 0; i < NUM_QUALIFIERS; i++) {
                    byte[] qualifier = toBytes("q" + i);
                    byte[] value = toBytes("value_" + i);
                    put.addColumn(family, qualifier, currentTimestamp, value);
                    qualifiers.add(Bytes.toString(qualifier));
                }
            }
            hTable.put(put);
            // verify
            Get get = new Get(Bytes.toBytes("multi_cf_ts_row"));
            Result result = hTable.get(get);
            for (String tableName : entry.getValue()) {
                byte[] family = toBytes(getColumnFamilyName(tableName));
                Assert.assertEquals(NUM_QUALIFIERS,
                        result.getFamilyMap(family).size());

                for (int i = 0; i < NUM_QUALIFIERS; i++) {
                    byte[] val = result.getValue(family, Bytes.toBytes("q" + i));
                    Assert.assertEquals( "value_" + i,
                            Bytes.toString(val));
                }
            }
        }

        {
            byte[] ROW = Bytes.toBytes("batch_row");
            List<Put> puts = new ArrayList<>();
            for (String tableName : entry.getValue()) {
                byte[] family = toBytes(getColumnFamilyName(tableName));
                Put put = new Put(ROW);
                for (int i = 0; i < NUM_QUALIFIERS; i++) {
                    put.addColumn(family,
                            Bytes.toBytes("q" + i),
                            Bytes.toBytes("value_" + i));
                }
                puts.add(put);
            }
            hTable.put(puts);
            // verify
            Get get = new Get(ROW);
            Result result = hTable.get(get);
            for (String tableName : entry.getValue()) {
                byte[] family = toBytes(getColumnFamilyName(tableName));
                Assert.assertEquals(NUM_QUALIFIERS,
                        result.getFamilyMap(family).size());

                for (int i = 0; i < NUM_QUALIFIERS; i++) {
                    byte[] val = result.getValue(family, Bytes.toBytes("q" + i));
                    Assert.assertEquals( "value_" + i,
                            Bytes.toString(val));
                }
            }
        }

        {
            long timestamp = System.currentTimeMillis();
            byte[] ROW = Bytes.toBytes("batch_ts_row");
            List<Put> puts = new ArrayList<>();
            for (String tableName : entry.getValue()) {
                byte[] family = toBytes(getColumnFamilyName(tableName));
                for (int i = 0; i < NUM_QUALIFIERS; i++) {
                    Put put = new Put(ROW);
                    put.addColumn(family,
                            Bytes.toBytes("q" + i),
                            timestamp,
                            Bytes.toBytes("value_" + i));
                    puts.add(put);
                }
            }
            hTable.put(puts);
            // verify
            Get get = new Get(ROW);
            Result result = hTable.get(get);
            for (String tableName : entry.getValue()) {
                byte[] family = toBytes(getColumnFamilyName(tableName));
                Assert.assertEquals(NUM_QUALIFIERS,
                        result.getFamilyMap(family).size());

                for (int i = 0; i < NUM_QUALIFIERS; i++) {
                    byte[] val = result.getValue(family, Bytes.toBytes("q" + i));
                    Assert.assertEquals( "value_" + i,
                            Bytes.toString(val));
                }
            }
        }

        {
            byte[] ROW = Bytes.toBytes("batch_row_2");
            List<Put> puts = new ArrayList<>();
            for (int i = 0; i < NUM_QUALIFIERS; i++) {
                Put put = new Put(ROW);
                for (String tableName : entry.getValue()) {
                    byte[] family = toBytes(getColumnFamilyName(tableName));
                    put.addColumn(family,
                        Bytes.toBytes("q" + i),
                        Bytes.toBytes("value_" + i));
                }
                puts.add(put);
            }
            hTable.put(puts);
            // verify
            Get get = new Get(ROW);
            Result result = hTable.get(get);
            for (String tableName : entry.getValue()) {
                byte[] family = toBytes(getColumnFamilyName(tableName));
                Assert.assertEquals(NUM_QUALIFIERS,
                        result.getFamilyMap(family).size());

                for (int i = 0; i < NUM_QUALIFIERS; i++) {
                    byte[] val = result.getValue(family, Bytes.toBytes("q" + i));
                    Assert.assertEquals( "value_" + i,
                            Bytes.toString(val));
                }
            }
        }
    }
}
