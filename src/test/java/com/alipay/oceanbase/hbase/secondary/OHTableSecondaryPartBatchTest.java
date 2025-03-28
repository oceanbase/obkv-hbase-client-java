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
import com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import com.alipay.oceanbase.hbase.util.TableTemplateManager;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.io.IOException;
import java.util.*;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;

public class OHTableSecondaryPartBatchTest {
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

    public static void testBatchPutImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        String family = getColumnFamilyName(tableName);
        List<Put> puts = new ArrayList<>();
        long timestamp = System.currentTimeMillis() - 100;
        int batchSize = 10;
        for (int i = 0; i < batchSize; i++) {
            Put put = new Put(toBytes("row-" + i));
            put.addColumn(toBytes(family), toBytes("col1"), timestamp ,toBytes("value-1-" + i));
            put.addColumn(toBytes(family), toBytes("col2"), timestamp, toBytes("value-2-" + i));
            put.addColumn(toBytes(family), toBytes("col3") ,toBytes("value-3-" + i));
            put.addColumn(toBytes(family), toBytes("col4"), toBytes("value-4-" + i));
            puts.add(put);
        }
        hTable.put(puts);

        // verify result
        long timestamp2 = System.currentTimeMillis();
        for (int i = 0; i < batchSize; i++) {
            Get get = new Get(toBytes("row-" + i));
            get.addColumn(toBytes(family), toBytes("col1"));
            get.addColumn(toBytes(family), toBytes("col2"));
            get.addColumn(toBytes(family), toBytes("col3"));
            get.addColumn(toBytes(family), toBytes("col4"));

            get.setMaxVersions(1);
            Result result = hTable.get(get);
            ObHTableTestUtil.Assert(tableName, ()->Assert.assertEquals(4, result.size()));
            for (Cell cell: result.listCells()) {
               Assert.assertEquals(family, Bytes.toString(CellUtil.cloneFamily(cell)));
               String Q = Bytes.toString(CellUtil.cloneQualifier(cell));
               if (Q.equals("col1")) {
                   Assert.assertEquals("value-1-" + i,Bytes.toString(CellUtil.cloneValue(cell)));
                   Assert.assertEquals(timestamp, cell.getTimestamp());
               } else if (Q.equals("col2")) {
                   Assert.assertEquals("value-2-" + i,Bytes.toString(CellUtil.cloneValue(cell)));
                   Assert.assertEquals(timestamp, cell.getTimestamp());
               } else if (Q.equals("col3")) {
                   Assert.assertEquals("value-3-" + i,Bytes.toString(CellUtil.cloneValue(cell)));
                   Assert.assertTrue(timestamp < cell.getTimestamp());
                    timestamp2 = cell.getTimestamp();
               } else if (Q.equals("col4")) {
                   Assert.assertEquals("value-4-" + i,Bytes.toString(CellUtil.cloneValue(cell)));
                   Assert.assertTrue(timestamp < cell.getTimestamp());
               } else {
                   Assert.fail();
               }
            }
        }

        // put exist key
        puts.clear();
        for (int i = 0; i < batchSize; i++) {
            Put put = new Put(toBytes("row-" + i));
            // update
            put.addColumn(toBytes(family), toBytes("col1"), timestamp ,toBytes("update-value-1-" + i));
            put.addColumn(toBytes(family), toBytes("col2"), timestamp, toBytes("update-value-2-" + i));
            // insert
            put.addColumn(toBytes(family), toBytes("col3") ,toBytes("update-value-3-" + i));
            put.addColumn(toBytes(family), toBytes("col4"), toBytes("update-value-4-" + i));
            puts.add(put);
        }
        hTable.put(puts);

        for (int i = 0; i < batchSize; i++) {
            Get get = new Get(toBytes("row-" + i));
            get.addColumn(toBytes(family), toBytes("col1"));
            get.addColumn(toBytes(family), toBytes("col2"));
            get.addColumn(toBytes(family), toBytes("col3"));
            get.addColumn(toBytes(family), toBytes("col4"));
            get.setMaxVersions();
            Result result = hTable.get(get);
            ObHTableTestUtil.Assert(tableName, ()->Assert.assertEquals(6, result.size()));
            for (Cell cell: result.listCells()) {
                Assert.assertEquals(family, Bytes.toString(CellUtil.cloneFamily(cell)));
                String Q = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (Q.equals("col1")) {
                    Assert.assertEquals("update-value-1-" + i,Bytes.toString(CellUtil.cloneValue(cell)));
                    Assert.assertEquals(timestamp, cell.getTimestamp());
                } else if (Q.equals("col2")) {
                    Assert.assertEquals("update-value-2-" + i,Bytes.toString(CellUtil.cloneValue(cell)));
                    Assert.assertEquals(timestamp, cell.getTimestamp());
                } else if (Q.equals("col3")) {
                    if (timestamp2 == cell.getTimestamp()) {
                        Assert.assertEquals("value-3-" + i,Bytes.toString(CellUtil.cloneValue(cell)));
                    } else {
                        Assert.assertEquals("update-value-3-" + i,Bytes.toString(CellUtil.cloneValue(cell)));
                        Assert.assertTrue(timestamp2 < cell.getTimestamp());
                    }
                } else if (Q.equals("col4")) {
                    if (timestamp2 == cell.getTimestamp()) {
                        Assert.assertEquals("value-4-" + i, Bytes.toString(CellUtil.cloneValue(cell)));
                    } else {
                        Assert.assertEquals("update-value-4-" + i, Bytes.toString(CellUtil.cloneValue(cell)));
                        Assert.assertTrue(timestamp2 < cell.getTimestamp());
                    }
                } else {
                    Assert.fail();
                }
            }
        }
        hTable.close();
    }

    public static void testMultiCFBatchPutImpl(Map.Entry<String, List<String>> entry) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(entry.getKey());
        hTable.init();
        int batchSize = 10;
        List<Put> puts = new ArrayList<>();
        String[] qualifier = new String[] {"col-1", "col-2", "col-3", "col-4"};
        String[] value = new String[] {"value-1", "value-2", "value-3", "value-4"};
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < batchSize; i++) {
            Put put = new Put(toBytes("row-" + i));
            for (String tableName : entry.getValue()) {
                String family = getColumnFamilyName(tableName);
                put.add(toBytes(family), toBytes(qualifier[0]), timestamp, toBytes(value[0]));
                put.add(toBytes(family), toBytes(qualifier[1]), timestamp, toBytes(value[1]));
                put.add(toBytes(family), toBytes(qualifier[2]), toBytes(value[2]));
                put.add(toBytes(family), toBytes(qualifier[3]), toBytes(value[3]));
            }
            puts.add(put);
        }
        hTable.put(puts);
        // verify initial put result
        for (int i = 0; i < batchSize; i++) {
            for (String tableName : entry.getValue()) {
                Get get = new Get(toBytes("row-" + i));
                get.setMaxVersions(1);
                String family = getColumnFamilyName(tableName);
                get.addColumn(toBytes(family), toBytes(qualifier[0]));
                get.addColumn(toBytes(family), toBytes(qualifier[1]));
                get.addColumn(toBytes(family), toBytes(qualifier[2]));
                get.addColumn(toBytes(family), toBytes(qualifier[3]));

                Result result = hTable.get(get);
                ObHTableTestUtil.Assert(tableName, ()->Assert.assertEquals(4, result.size()));
                Assert.assertEquals(value[0], Bytes.toString(result.getValue(toBytes(family), toBytes(qualifier[0]))));
                Assert.assertEquals(timestamp, result.getColumnLatestCell(toBytes(family), toBytes(qualifier[0])).getTimestamp());
                Assert.assertEquals(value[1], Bytes.toString(result.getValue(toBytes(family), toBytes(qualifier[1]))));
                Assert.assertEquals(timestamp, result.getColumnLatestCell(toBytes(family), toBytes(qualifier[1])).getTimestamp());
                Assert.assertEquals(value[2], Bytes.toString(result.getValue(toBytes(family), toBytes(qualifier[2]))));
                Assert.assertTrue(timestamp < result.getColumnLatestCell(toBytes(family), toBytes(qualifier[2])).getTimestamp());
                Assert.assertEquals(value[3], Bytes.toString(result.getValue(toBytes(family), toBytes(qualifier[3]))));
                Assert.assertTrue(timestamp < result.getColumnLatestCell(toBytes(family), toBytes(qualifier[3])).getTimestamp());
            }
        }

        // put exist key
        puts.clear();
        String[] updateValue = new String[] {"update-value-1", "update-value-2", "update-value-3", "update-value-4"};
        for (int i = 0; i < batchSize; i++) {
            Put put = new Put(toBytes("row-" + i));
            for (String tableName : entry.getValue()) {
                String family = getColumnFamilyName(tableName);
                put.add(toBytes(family), toBytes(qualifier[0]), timestamp, toBytes(updateValue[0]));
                put.add(toBytes(family), toBytes(qualifier[1]), timestamp, toBytes(updateValue[1]));
                put.add(toBytes(family), toBytes(qualifier[2]), toBytes(updateValue[2]));
                put.add(toBytes(family), toBytes(qualifier[3]), toBytes(updateValue[3]));
            }
            puts.add(put);
        }
        hTable.put(puts);

        // verify update result
        for (int i = 0; i < batchSize; i++) {
            for (String tableName : entry.getValue()) {
                Get get = new Get(toBytes("row-" + i));
                get.setMaxVersions();  // Get both versions
                String family = getColumnFamilyName(tableName);
                get.addColumn(toBytes(family), toBytes(qualifier[0]));
                get.addColumn(toBytes(family), toBytes(qualifier[1]));
                get.addColumn(toBytes(family), toBytes(qualifier[2]));
                get.addColumn(toBytes(family), toBytes(qualifier[3]));

                // Verify values
                Result result = hTable.get(get);
                ObHTableTestUtil.Assert(tableName, ()->Assert.assertEquals(6, result.size()));
                // qualifier[0] - 1 versions
                List<Cell> col0Cells = result.getColumnCells(toBytes(family), toBytes(qualifier[0]));
                Assert.assertEquals(1, col0Cells.size());
                Assert.assertEquals(updateValue[0], Bytes.toString(CellUtil.cloneValue(col0Cells.get(0))));
                Assert.assertEquals(timestamp, col0Cells.get(0).getTimestamp());

                // qualifier[1] - 1 versions
                List<Cell> col1Cells = result.getColumnCells(toBytes(family), toBytes(qualifier[1]));
                Assert.assertEquals(1, col1Cells.size());
                Assert.assertEquals(updateValue[1], Bytes.toString(CellUtil.cloneValue(col1Cells.get(0))));
                Assert.assertEquals(timestamp, col1Cells.get(0).getTimestamp());

                // qualifier[2] - 2 version
                List<Cell> col2Cells = result.getColumnCells(toBytes(family), toBytes(qualifier[2]));
                Assert.assertEquals(2, col2Cells.size());
                Assert.assertEquals(updateValue[2], Bytes.toString(CellUtil.cloneValue(col2Cells.get(0))));
                Assert.assertTrue(timestamp < col2Cells.get(0).getTimestamp());
                Assert.assertEquals(value[2], Bytes.toString(CellUtil.cloneValue(col2Cells.get(1))));

                // qualifier[3] - 2 version
                List<Cell> col3Cells = result.getColumnCells(toBytes(family), toBytes(qualifier[3]));
                Assert.assertEquals(2, col3Cells.size());
                Assert.assertEquals(updateValue[3], Bytes.toString(CellUtil.cloneValue(col3Cells.get(0))));
                Assert.assertTrue(timestamp < col3Cells.get(0).getTimestamp());
                Assert.assertEquals(value[3], Bytes.toString(CellUtil.cloneValue(col3Cells.get(1))));

            }
        }

        hTable.close();
    }

    public static void testBatchGetImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        String keyPrefix = "putKey_";
        String family = getColumnFamilyName(tableName);
        String[] columns = {"putColumn1", "putColumn2", "putColumn3"};
        long curTs = System.currentTimeMillis();
        long[] ts = {curTs, curTs - 100};
        String valuePrefix = "value_";
        int batchSize = 10;
        // prepare data
        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            Put put = new Put(toBytes(keyPrefix + i));
            for (int j = 0; j < columns.length; j++) {
                put.add(toBytes(family), toBytes(columns[j]), ts[0], toBytes(valuePrefix + "1_" +i));
                put.add(toBytes(family), toBytes(columns[j]), ts[1], toBytes(valuePrefix + "2_" +i));
            }
            puts.add(put);
        }
        hTable.put(puts);
        // start to batch get test
        List<Get> gets = new ArrayList<>();
        // 1. get specify column
        {
            int index = 0;
            for (int i = 0; i < batchSize; i++) {
                Get get = new Get(toBytes(keyPrefix + i));
                get.addColumn(family.getBytes(), columns[index].getBytes());
                get.setMaxVersions();
                gets.add(get);
            }
            Result[] batchResults = hTable.get(gets);
            Assert.assertEquals(batchSize, batchResults.length);
            for (int i = 0; i < batchSize; i++) {
                Result r = batchResults[i];
                Assert.assertEquals(2, r.size());
                List<Cell> cells = r.listCells();
                for (int j = 0; j < r.size(); j++) {
                    Cell cell = cells.get(j);
                    Assert.assertEquals(keyPrefix+i, Bytes.toString(cell.getRow()));
                    Assert.assertEquals(columns[index], Bytes.toString(cell.getQualifier()));
                    Assert.assertEquals(ts[j], cell.getTimestamp());
                    if (j == 0) {
                        Assert.assertEquals(valuePrefix+"1_"+i, Bytes.toString(cell.getValue()));
                    } else {
                        Assert.assertEquals(valuePrefix+"2_"+i, Bytes.toString(cell.getValue()));
                    }
                }
            }
        }
        gets.clear();
        // 2. get do not specify column
        {
            for (int i = 0; i < batchSize; i++) {
                Get get = new Get(toBytes(keyPrefix + i));
                get.setMaxVersions();
                get.addFamily(family.getBytes());
                gets.add(get);
            }
            Result[] batchResults = hTable.get(gets);
            Assert.assertEquals(batchSize, batchResults.length);
            for (int i = 0; i < batchSize; i++) {
                Result r = batchResults[i];
                Assert.assertEquals(6, r.size());
                for (int j = 0; j < columns.length; j++) {
                   List<Cell> cells = r.getColumnCells(toBytes(family), toBytes(columns[j]));
                   Assert.assertEquals(2, cells.size());
                   for (int k = 0; k < cells.size(); k++) {
                       Assert.assertEquals(ts[k], cells.get(k).getTimestamp());
                       if (k == 0) {
                           Assert.assertEquals(valuePrefix+"1_"+i, Bytes.toString(cells.get(k).getValue()));
                       } else {
                           Assert.assertEquals(valuePrefix+"2_"+i, Bytes.toString(cells.get(k).getValue()));
                       }
                   }
                }
            }
        }
        // 3. get specify version
        gets.clear();
        {
            for (int i = 0; i < batchSize; i++) {
                Get get = new Get(toBytes(keyPrefix + i));
                get.addFamily(family.getBytes());
                get.setMaxVersions(2);
                gets.add(get);
            }
            Result[] batchResults = hTable.get(gets);
            Assert.assertEquals(batchSize, batchResults.length);
            for (int i = 0; i < batchSize; i++) {
                Result r = batchResults[i];
                Assert.assertEquals(6, r.size());
                for (int j = 0; j < columns.length; j++) {
                    List<Cell> cells = r.getColumnCells(family.getBytes(), toBytes(columns[j]));
                    Assert.assertEquals(2, cells.size());
                    for (int k = 0; k < cells.size(); k++) {
                        Assert.assertEquals(ts[k], cells.get(k).getTimestamp());
                        if (k == 0) {
                            Assert.assertEquals(valuePrefix+"1_"+i, Bytes.toString(cells.get(k).getValue()));
                        } else {
                            Assert.assertEquals(valuePrefix+"2_"+i, Bytes.toString(cells.get(k).getValue()));
                        }
                    }
                }
            }
        }
        // 4. get specify time range
        gets.clear();
        {
            for (int i = 0; i < batchSize; i++) {
                Get get = new Get(toBytes(keyPrefix + i));
                get.addFamily(family.getBytes());
                get.setMaxVersions(2);
                get.setTimeStamp(ts[1]);
                gets.add(get);
            }
            Result[] batchResults = hTable.get(gets);
            Assert.assertEquals(batchSize, batchResults.length);
            for (int i = 0; i < batchSize; i++) {
                Result r = batchResults[i];
                Assert.assertEquals(3, r.size());
                for (int j = 0; j < columns.length; j++) {
                    List<Cell> cells = r.getColumnCells(family.getBytes(), toBytes(columns[j]));
                    Assert.assertEquals(1, cells.size());
                    Assert.assertEquals(ts[1], cells.get(0).getTimestamp());
                    Assert.assertEquals(valuePrefix+"2_"+i, Bytes.toString(cells.get(0).getValue()));
                }
            }
        }
        // 5. get specify filter
        gets.clear();
        {
            for (int i = 0; i < batchSize; i++) {
                Get get = new Get(toBytes(keyPrefix+i));
                get.addFamily(family.getBytes());
                get.setMaxVersions(2);
                ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                        new BinaryComparator(toBytes(valuePrefix+"2_"+i)));
                get.setFilter(valueFilter);
                gets.add(get);
            }
            Result[] batchResults = hTable.get(gets);
            Assert.assertEquals(batchSize, batchResults.length);
            for (int i = 0; i < batchSize; i++) {
                Result r = batchResults[i];
                Assert.assertEquals(3, r.size());
                for (int j = 0; j < columns.length; j++) {
                    List<Cell> cells = r.getColumnCells(family.getBytes(), toBytes(columns[j]));
                    Assert.assertEquals(1, cells.size());
                    Assert.assertEquals(ts[1], cells.get(0).getTimestamp());
                    Assert.assertEquals(valuePrefix+"2_"+i, Bytes.toString(cells.get(0).getValue()));
                }
            }
        }
        hTable.close();
    }

    public static void testMixBatchImpl(String tableName) throws Exception {
        byte[] family = getColumnFamilyName(tableName).getBytes();
        byte[][] keys = new byte[][]{"key1".getBytes(), "key2".getBytes()};
        byte[][] qualifiers = new byte[][]{"col1".getBytes(), "col2".getBytes()};
        byte[][] values = new byte[][]{"value1".getBytes(), "value2".getBytes()};
        long curTs = System.currentTimeMillis();
        long[] ts = {curTs, curTs - 1000};
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        List<Row> batchLsit = new LinkedList<>();
        // 0. load data
        for (int i = 0; i < keys.length; i++) {
            Put put = new Put(keys[i]);
            for (int j = 0; j < qualifiers.length; j++) {
                for (int k = 0; k < values.length; k++) {
                    put.add(family, qualifiers[j], ts[k], values[k]);
                }
            }
            batchLsit.add(put);
        }
        hTable.batch(batchLsit);
        // 1. get + put + get
        batchLsit.clear();
        {
            // get old result
            Get get1 = new Get(keys[0]);
            get1.setMaxVersions();
            get1.addFamily(family);
            // get old result
            Get get2 = new Get(keys[1]);
            get2.setMaxVersions(1);
            get2.addColumn(family, qualifiers[1]);

            // put new value
            byte[] newValue = "new_value".getBytes();
            Put put1 = new Put(keys[0]);
            put1.add(family, qualifiers[0], newValue);
            // put new value
            Put put2 = new Put(keys[1]);
            put2.add(family, qualifiers[1], newValue);
            // get new result
            Get get3 = new Get(keys[0]);
            get3.setMaxVersions(1);
            get3.addColumn(family, qualifiers[0]);

            // get new result
            Get get4 = new Get(keys[1]);
            get4.setMaxVersions(1);
            get4.addColumn(family, qualifiers[1]);

            // execute
            batchLsit.addAll(Arrays.asList(get1 ,get2, put1, put2, get3, get4));
            Object[] results = hTable.batch(batchLsit);
            // verify result
            Assert.assertEquals(6, results.length);
            // get1
            Result get1Result = (Result) results[0];
            Assert.assertEquals(4, get1Result.raw().length);
            List<Cell> k0cq0 = get1Result.getColumnCells(family, qualifiers[0]);
            Assert.assertEquals(2, k0cq0.size());
            AssertKeyValue(Bytes.toString(keys[0]), Bytes.toString(qualifiers[0]), ts[0], Bytes.toString(values[0]), k0cq0.get(0));
            AssertKeyValue(Bytes.toString(keys[0]), Bytes.toString(qualifiers[0]), ts[1], Bytes.toString(values[1]), k0cq0.get(1));
            List<Cell> k0cq1 = get1Result.getColumnCells(family, qualifiers[1]);
            Assert.assertEquals(2, k0cq1.size());
            AssertKeyValue(Bytes.toString(keys[0]), Bytes.toString(qualifiers[1]), ts[0], Bytes.toString(values[0]), k0cq1.get(0));
            AssertKeyValue(Bytes.toString(keys[0]), Bytes.toString(qualifiers[1]), ts[1], Bytes.toString(values[1]), k0cq1.get(1));
            // get2
            Result get2Result = (Result) results[1];
            Assert.assertEquals(1, get2Result.raw().length);
            List<Cell> k1cq1 = get2Result.getColumnCells(family, qualifiers[1]);
            Assert.assertEquals(1, k1cq1.size());
            AssertKeyValue(Bytes.toString(keys[1]), Bytes.toString(qualifiers[1]), ts[0], Bytes.toString(values[0]), k1cq1.get(0));
            // get3
            Result get3Result = (Result) results[4];
            Assert.assertEquals(1, get3Result.raw().length);
            k0cq0 = get3Result.getColumnCells(family, qualifiers[0]);
            Assert.assertEquals(1, k0cq0.size());
            AssertKeyValue(Bytes.toString(keys[0]), Bytes.toString(qualifiers[0]), Bytes.toString(newValue), k0cq0.get(0));
            // get4
            Result get4Result = (Result) results[5];
            Assert.assertEquals(1, get4Result.raw().length);
            k1cq1 = get4Result.getColumnCells(family, qualifiers[1]);
            Assert.assertEquals(1, k1cq1.size());
            AssertKeyValue(Bytes.toString(keys[1]), Bytes.toString(qualifiers[1]), Bytes.toString(newValue), k1cq1.get(0));
        }
        // 2. delete + get + delete + get
        batchLsit.clear();
        {
            // delete all version keys[0]-qualifiers[0]
            Delete delete1 =  new Delete(keys[0]);
            delete1.addColumns(family, qualifiers[0]);

            // get family: keys[0]-qualifier[1] may have two versions and keys[0]-qualifier[0] have been deleted
            Get get1 = new Get(keys[0]);
            get1.addFamily(family);
            get1.setMaxVersions();
            // delete all version keys[0]-qualifier[1]
            Delete delete2 =  new Delete(keys[0]);
            delete2.addColumns(family, qualifiers[1]);
            // keys[0] may not have results
            Get get2 = new Get(keys[0]);
            get2.addFamily(family);
            get2.setMaxVersions();
            // execute
            batchLsit.addAll(Arrays.asList(delete1, get1, delete2, get2));
            Object[] results = hTable.batch(batchLsit);
            // verify result
            Assert.assertEquals(4, results.length);
            Result get1Result = (Result) results[1];
            ObHTableTestUtil.Assert(tableName, ()->Assert.assertEquals(2, get1Result.size()));
            AssertKeyValue(Bytes.toString(keys[0]), Bytes.toString(family), Bytes.toString(qualifiers[1]),
                    ts[0], Bytes.toString(values[0]), get1Result.listCells().get(0));
            AssertKeyValue(Bytes.toString(keys[0]), Bytes.toString(family), Bytes.toString(qualifiers[1]),
                    ts[1], Bytes.toString(values[1]), get1Result.listCells().get(1));
            Result get2Result = (Result) results[3];
            Assert.assertEquals(0, get2Result.size());
        }
        // 3. put + delete + get
        batchLsit.clear();
        {
            Put put1 = new Put(keys[0]);
            put1.add(family, qualifiers[0], "new_new_value".getBytes());
            put1.add(family, qualifiers[1], "new_new_value".getBytes());
            Put put2 = new Put(keys[1]);
            put2.add(family, qualifiers[0], "new_new_value".getBytes());
            put2.add(family, qualifiers[1], "new_new_value".getBytes());
            Delete delete = new Delete(keys[0]);
            delete.addColumns(family, qualifiers[0]);
            Get get1 = new Get(keys[0]);
            get1.setMaxVersions(1);
            get1.addFamily(family);

            Get get2 = new Get(keys[1]);
            get2.setMaxVersions(1);
            get2.addFamily(family);

            batchLsit.addAll(Arrays.asList(put1, put2, delete, get1, get2));
            Object[] results = hTable.batch(batchLsit);
            Assert.assertEquals(5, results.length);
            Result getResult1 = (Result) results[3];
            Assert.assertEquals(1, getResult1.size());
            AssertKeyValue(Bytes.toString(keys[0]), Bytes.toString(qualifiers[1]),
                    "new_new_value", getResult1.listCells().get(0));
            Result getResult2 = (Result) results[4];
            Assert.assertEquals(2, getResult2.size());
            AssertKeyValue(Bytes.toString(keys[1]), Bytes.toString(qualifiers[0]),
                    "new_new_value", getResult2.listCells().get(0));
            AssertKeyValue(Bytes.toString(keys[1]), Bytes.toString(qualifiers[1]),
                    "new_new_value", getResult2.listCells().get(1));
        }
        hTable.close();
    }

    public static void testMultiCFBatchGetImpl(Map.Entry<String, List<String>> entry) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(entry.getKey()));
        hTable.init();
        String keyPrefix = "putKey_";
        String[] columns = {"putColumn1", "putColumn2", "putColumn3"};
        String[] values = {"version1_", "version2_"}; // each column have two versions
        String latestValue = values[1];
        List<String> tableNames = entry.getValue();
        long timestamp = System.currentTimeMillis();
        long[] ts = {timestamp, timestamp+1};
        long lastTs = ts[1];
        int batchSize = 10;
        // 0. prepare data
        List<Put> puts = new ArrayList<>();
        for (int k = 0; k < batchSize; k++) {
            String key = keyPrefix+k;
            Put put = new Put(toBytes(key));
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                for (int i = 0; i < values.length; i++) {
                    for (int j = 0; j < columns.length; j++) {
                        put.add(family.getBytes(), columns[j].getBytes(), ts[i], toBytes(values[i]+k));
                    }
                }
            }
            puts.add(put);
        }
        hTable.put(puts);
        // 1. get specify column
        {
            List<Get> gets = new ArrayList<>();
            int columnIndex = 0;
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                for (String tableName : tableNames) {
                    Get get = new Get(key.getBytes());
                    String family = getColumnFamilyName(tableName);
                    get.addColumn(family.getBytes(), columns[columnIndex].getBytes());
                    gets.add(get);
                }
            }
            Result[] results = hTable.get(gets);
            Assert.assertEquals(gets.size(), results.length);
            for (int i = 0; i < gets.size(); i++) {
                int idx = i / tableNames.size();
                String key = keyPrefix + idx;
                Result res = results[i];
                Assert.assertEquals(1, res.size());
                AssertKeyValue(key, columns[columnIndex], lastTs, latestValue + idx, res.rawCells()[0]);
            }
        }
        // 2. get do not specify column
        {
            List<Get> gets = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                Get get = new Get(key.getBytes());
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    get.addFamily(family.getBytes());
                }
                gets.add(get);
            }
            Result[] results = hTable.get(gets);
            Assert.assertEquals(batchSize, results.length);
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                Result result = results[i];
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    for (int j = 0; j < columns.length; j++) {
                        Cell cell = result.getColumnCells(family.getBytes(), columns[j].getBytes()).get(0);
                        AssertKeyValue(key, columns[j], lastTs, latestValue + i, cell);
                    }
                }
            }
        }
        // 3. get do not specify column family
        {
            List<Get> gets = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                Get get = new Get(key.getBytes());
                gets.add(get);
            }
            Result[] results = hTable.get(gets);
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                Result result = results[i];
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    for (int j = 0; j < columns.length; j++) {
                        Cell cell = result.getColumnCells(family.getBytes(), columns[j].getBytes()).get(0);
                        AssertKeyValue(key, columns[j], lastTs, latestValue + i, cell);
                    }
                }
            }
        }
        // 4. get specify multi cf and column
        {
            List<Get> gets = new ArrayList<>();
            int columnIndex = 0;
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                Get get = new Get(key.getBytes());
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    get.addColumn(family.getBytes(), columns[columnIndex].getBytes());
                }
                gets.add(get);
            }
            Result[] results = hTable.get(gets);
            Assert.assertEquals(batchSize, results.length);
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                Result res = results[i];
                Assert.assertEquals(tableNames.size(), res.size());
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    AssertKeyValue(key, columns[columnIndex], lastTs, latestValue + i,
                            res.getColumnCells(family.getBytes(), columns[columnIndex].getBytes()).get(0));

                }
            }
        }
        // 5. get specify multi cf and versions
        {
            List<Get> gets = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                Get get = new Get(key.getBytes());
                get.setMaxVersions(2);
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    get.addFamily(family.getBytes());
                }
                gets.add(get);
            }
            Result[] results = hTable.get(gets);
            Assert.assertEquals(batchSize, results.length);
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                Result res = results[i];
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    for (int j = 0; j < columns.length; j++) {
                        List<Cell> cells = res.getColumnCells(family.getBytes(), columns[j].getBytes());
                        Assert.assertEquals(2, cells.size());
                        for (int k = ts.length - 1; k >= 0; k--) {
                            AssertKeyValue(key, family, columns[j], ts[k], values[k] + i, cells.get(ts.length - k - 1));
                        }
                    }
                }
            }
        }
        // 6. get specify multi cf and set timestamp
        {
            List<Get> gets = new ArrayList<>();
            int columnIndex = 0;
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                Get get = new Get(key.getBytes());
                get.setTimeStamp(ts[1]);
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    get.addColumn(family.getBytes(), columns[columnIndex].getBytes());
                }
                gets.add(get);
            }
            Result[] results = hTable.get(gets);
            Assert.assertEquals(batchSize, results.length);
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                Result res = results[i];
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    AssertKeyValue(key, columns[columnIndex], ts[1], latestValue + i,
                            res.getColumnCells(family.getBytes(), columns[columnIndex].getBytes()).get(0));
                }
            }
        }
        // 7. get specify multi cf and filter
        {
            List<Get> gets = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                Get get = new Get(key.getBytes());
                get.setMaxVersions(2);
                ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                        new BinaryComparator(toBytes(values[0] + i)));
                get.setFilter(valueFilter);
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    get.addFamily(family.getBytes());
                }
                gets.add(get);
            }
            Result[] results = hTable.get(gets);
            Assert.assertEquals(batchSize, results.length);
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                Result res = results[i];
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    for (int j = 0; j < columns.length; j++) {
                        AssertKeyValue(key, columns[j], ts[0], values[0] + i,
                                res.getColumnCells(family.getBytes(), columns[j].getBytes()).get(0));
                    }
                }
            }
        }
    }

    public static void testMultiCFMixBatchImpl(Map.Entry<String, List<String>> entry) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(entry.getKey()));
        hTable.init();
        String keyPrefix = "Key_";
        String[] columns = {"Column1", "Column2"};
        String[] values = {"version1_", "version2_"}; // each column have two versions
        String latestValue = values[1];
        long timestamp = System.currentTimeMillis();
        long[] ts = {timestamp, timestamp+1};
        long lastTs = ts[1];
        int batchSize = 10;
        List<String> tableNames = entry.getValue();

        // 0. prepare data
        {
            List<Put> puts = new ArrayList<>();
            for (int k = 0; k < batchSize; k++) {
                String key = keyPrefix + k;
                Put put = new Put(toBytes(key));
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    for (int i = 0; i < values.length; i++) {
                        for (int j = 0; j < columns.length; j++) {
                            put.add(family.getBytes(), columns[j].getBytes(), ts[i], toBytes(values[i] + k));
                        }
                    }
                }
                puts.add(put);
            }
            hTable.put(puts);
        }
        String[] families = getAllFamilies(tableNames);
        // 1. get + put + get
        {
            List<Row> batchList = new LinkedList<>();
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                batchList.add(createGetOp(key, families, columns[0], ts[1]));
                batchList.add(createPutOp(key, families, columns[0], ts[1], "new_value_"+i));
                batchList.add(createGetOp(key, families, columns[0], ts[1]));
            }
            Object[] results = hTable.batch(batchList);
            Assert.assertEquals(batchSize*3, results.length);
            for (int i = 0; i < batchSize; i++) {
                Result get1Result = (Result) results[3*i+0];
                checkResult(keyPrefix+i, families, columns[0], ts[1], values[1]+i, get1Result);
                Result get2Result = (Result) results[3*i+2];
                checkResult(keyPrefix+i, families, columns[0], ts[1], "new_value_"+i, get2Result);
            }
        }

        // 2. delete + get + delete + get
        {
            List<Row> batchList = new LinkedList<>();
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                batchList.add(createDeleteOp(key, families, columns[1], ts[0]));
                batchList.add(createGetOp(key, families, columns[1], ts[0]));
                batchList.add(createGetOp(key, families, columns[0], ts[1]));
                batchList.add(createDeleteOp(key, families, columns[0], ts[1]));
                batchList.add(createGetOp(key, families, columns[0], ts[1]));
            }
            Object[] results = hTable.batch(batchList);
            Assert.assertEquals(batchSize*5, results.length);
            for (int i = 0; i < batchSize; i++) {
                Result get1Result = (Result) results[5*i+1];
                Assert.assertEquals(0, get1Result.size());
                Result get2Result = (Result) results[5*i+2];
                checkResult(keyPrefix+i, families, columns[0], ts[1], "new_value_"+i, get2Result);
                Result get3Result = (Result) results[5*i+4];
                Assert.assertEquals(0, get3Result.size());
            }
        }

        // 3. put + delete + get
        {
            List<Row> batchList = new LinkedList<>();
            for (int i = 0; i < batchSize; i++) {
                String key = keyPrefix + i;
                batchList.add(createPutOp(key, families, columns[0], ts[1], "new_new_value"+i));
                batchList.add(createGetOp(key, families, columns[0], ts[1]));
                batchList.add(createDeleteOp(key, families, columns[0], ts[1]));
                batchList.add(createGetOp(key, families, columns[0], ts[1]));
            }
            Object[] results = hTable.batch(batchList);
            Assert.assertEquals(batchSize*4, results.length);
            for (int i = 0; i < batchSize; i++) {
                Result get1Result = (Result) results[4*i+1];
                checkResult(keyPrefix+i, families, columns[0], ts[1], "new_new_value"+i, get1Result);
                Result get2Result = (Result) results[4*i+3];
                Assert.assertEquals(0, get2Result.size());
            }
        }
        hTable.close();
    }

    private static String[] getAllFamilies(List<String> tableNames) throws Exception {
        String[] families = new String[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            families[i] = getColumnFamilyName(tableNames.get(i));
        }
        return families;
    }

    private static void checkResult(String key, String[] families, String qualifier,
                               long ts, String value, Result result) {
        Assert.assertEquals(families.length, result.size());
        for (String family: families) {
            Cell cell = result.getColumnCells(family.getBytes(), qualifier.getBytes()).get(0);
            AssertKeyValue(key, family, qualifier, ts, value, cell);
        }
    }


    private static Put createPutOp(String key, String[] families, String qualifier,
                                   long ts, String value) {
        if (families == null || qualifier == null || value == null) {
            throw new IllegalArgumentException("input parameters is illegal");
        }
        Put put = new Put(key.getBytes());
        for (int i = 0; i < families.length; i++) {
            put.add(families[i].getBytes(), qualifier.getBytes(), ts, value.getBytes());
        }
        return put;
    }

    private static Delete createDeleteOp(String key, String[] families, String qualifier,
                                         long ts) {
        if (families == null || qualifier == null) {
            throw new IllegalArgumentException("input parameters is illegal");
        }
        Delete delete = new Delete(key.getBytes());
        for (int i = 0; i < families.length; i++) {
            delete.addColumn(families[i].getBytes(), qualifier.getBytes(), ts);
        }
        return delete;
    }

    private static Get createGetOp(String key, String[] families, String qualifier,
                                   long ts) throws IOException {
        if (families == null || qualifier == null) {
            throw new IllegalArgumentException("input parameters is illegal");
        }
        Get get = new Get(key.getBytes());
        for (int i = 0; i < families.length; i++) {
            get.addColumn(families[i].getBytes(), qualifier.getBytes());
            get.setTimeStamp(ts);
        }
        return get;
    }

    @Test
    public void testBatchPut() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartBatchTest::testBatchPutImpl);
    }

    @Test
    public void testMixBatch() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartBatchTest::testMixBatchImpl);
        truncateTables(ObHTableTestUtil.getConnection(), tableNames);
    }

    @Test
    public void testMultiCFMixBatch() throws Throwable {
        FOR_EACH(group2tableNames, OHTableSecondaryPartBatchTest::testMultiCFMixBatchImpl);
        truncateTables(ObHTableTestUtil.getConnection(), group2tableNames);
    }

    @Test
    public void testMultiCFPut() throws Throwable {
        FOR_EACH(group2tableNames, OHTableSecondaryPartBatchTest::testMultiCFBatchPutImpl);
    }

    @Test
    public void testBatchGet() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartBatchTest::testBatchGetImpl);
    }

    @Test
    public void testMultiCFGet() throws Throwable {
        FOR_EACH(group2tableNames, OHTableSecondaryPartBatchTest::testMultiCFBatchGetImpl);
    }
}
