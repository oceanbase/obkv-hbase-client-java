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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.util.*;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHTableSecondaryPartBatchTest {
    private static List<String> tableNames = new LinkedList<String>();
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


    public static void testMultiCFBatchGetImpl(Map.Entry<String, List<String>> entry) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(entry.getKey()));
        hTable.init();
        // prepare data
    }

    @Test
    public void testBatchPut() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartBatchTest::testBatchPutImpl);
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
