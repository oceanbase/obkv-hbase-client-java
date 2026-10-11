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
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.util.*;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static com.alipay.oceanbase.hbase.util.TableTemplateManager.TIMESERIES_TABLES;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;

public class OHTableTimeSeriesScanTest {
    private static List<String>              tableNames       = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = new LinkedHashMap<>();

    @BeforeClass
    public static void before() throws Exception {
        openDistributedExecute();
        for (TableTemplateManager.TableType type : TIMESERIES_TABLES) {
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

    public static void testScanImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        // 0. prepare data
        // putKey1 putColumn1 putValue1,ts
        // putKey1 putColumn1 putValue2,ts+1
        // putKey1 putColumn2 putValue1,ts
        // putKey1 putColumn2 putValue2,ts+1
        // ...
        String family = getColumnFamilyName(tableName);
        long ts = System.currentTimeMillis();

        String keys[] = {"putKey1", "putKey2", "putKey3"};
        String keys1[] = {"putKey1", "putKey2"};
        String keys2[] = {"putKey3"};
        String endKey = "putKey4";
        String columns[] = {"putColumn1", "putColumn2"};
        String values[] = {"putValue1", "putValue2"};
        long tss[] = {ts, ts + 1};

        for (String key : keys1) {
            Put put = new Put(toBytes(key));
            for (String column : columns) {
                for (int i = 0; i < values.length; i++) {
                    put.addColumn(family.getBytes(), column.getBytes(), tss[i], values[i].getBytes());
                }
            }
            hTable.put(put);
        }

        for (String key : keys2) {
            for (String column : columns) {
                for (int i = 0; i < values.length; i++) {
                    Put put = new Put(toBytes(key));
                    put.addColumn(family.getBytes(), column.getBytes(), tss[i], values[i].getBytes());
                    hTable.put(put);
                }
            }
        }

        // 1. scan specify column
        {
            Scan scan = new Scan(keys[0].getBytes(), endKey.getBytes());
            scan.addColumn(family.getBytes(), columns[0].getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            Cell cells[] = getCellsFromScanner(scanner).toArray(new Cell[0]);
            Assert.assertEquals(keys.length*values.length, cells.length);
            sortCells(cells);
            int idx = 0;
            for (String key : keys) {
                for (int i = values.length-1; i >= 0; i--) {
                    AssertKeyValue(key, columns[0], tss[i], values[i], cells[idx++]);
                }
            }
        }

        // 2. scan do not specify column
        {
            Scan scan = new Scan(keys[0].getBytes(), endKey.getBytes());
            scan.addFamily(family.getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            Cell cells[] = getCellsFromScanner(scanner).toArray(new Cell[0]);
            Assert.assertEquals(keys.length*columns.length*values.length, cells.length);
            sortCells(cells);
            int idx = 0;
            for (String key : keys) {
                for (String column : columns) {
                    for (int i = values.length-1; i >= 0; i--) {
                        AssertKeyValue(key, column, tss[i], values[i], cells[idx++]);
                    }
                }
            }
        }

        // 3. scan specify time range
        {
            Scan scan = new Scan(keys[0].getBytes(), endKey.getBytes());
            scan.setTimeStamp(tss[1]);
            scan.addFamily(family.getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            Cell cells[] = getCellsFromScanner(scanner).toArray(new Cell[0]);
            assertEquals(keys.length * columns.length, cells.length);
            sortCells(cells);
            int idx = 0;
            for (String key : keys) {
                for (String column : columns) {
                    AssertKeyValue(key, column, tss[1], values[1], cells[idx++]);
                }
            }
        }


        // 4. scan using setStartRow/setEndRow
        {
            Scan scan = new Scan();
            scan.addFamily(family.getBytes());
            scan.setStartRow(keys[0].getBytes());
            scan.setStopRow(endKey.getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            Cell cells[] = getCellsFromScanner(scanner).toArray(new Cell[0]);
            assertEquals(keys.length * columns.length * values.length, cells.length);
            sortCells(cells);
            int idx = 0;
            for (String key : keys) {
                for (String column : columns) {
                    for (int i = values.length-1; i >= 0; i--) {
                        AssertKeyValue(key, column, tss[i], values[i], cells[idx++]);
                    }
                }
            }
        }

        // 5. scan using batch
        {
            int batchSize = 2;
            Scan scan = new Scan(keys[0].getBytes(), endKey.getBytes());
            scan.addFamily(family.getBytes());
            scan.setBatch(batchSize);
            try {
                ResultScanner scanner = hTable.getScanner(scan);
            } catch (Exception e) {
                Assert.assertTrue(e.getCause().getMessage()
                    .contains("timeseries hbase table with batch query not supported"));
            }
            
        }

        // 7. scan using setAllowPartialResults/setAllowPartialResults
        {
            Scan scan = new Scan(keys[0].getBytes(), endKey.getBytes());
            scan.addFamily(family.getBytes());
            scan.setMaxResultSize(10);
            scan.setAllowPartialResults(true);
            try {
                ResultScanner scanner = hTable.getScanner(scan);
            } catch (Exception e) {
                Assert.assertTrue(e.getCause().getMessage()
                    .contains("timeseries hbase table with allow partial results query not supported"));
            }
        }

        // 8. scan in reverse
        {
            Scan scan = new Scan(keys[2].getBytes(), keys[0].getBytes());
            scan.addFamily(family.getBytes());
            scan.setReversed(true);
            try {
                ResultScanner scanner = hTable.getScanner(scan);
            } catch (Exception e) {
                Assert.assertTrue(e.getCause().getMessage()
                        .contains("timeseries hbase table with reverse query not supported"));
            }
        }
    }

    @Test
    public void testScan() throws Throwable {
        FOR_EACH(tableNames, OHTableTimeSeriesScanTest::testScanImpl);
    }

    @Test
    public void testScanWithLimit() throws Throwable {
        FOR_EACH(tableNames, OHTableTimeSeriesScanTest::testScanWithLimit);
    }

    public static void testScanWithLimit(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        // 准备测试数据
        String family = getColumnFamilyName(tableName);
        long ts = System.currentTimeMillis();
        
        // 准备更多的测试数据：5个key，每个key有3个列，每个列有4个版本
        String keys[] = {"limitKey1", "limitKey2", "limitKey3", "limitKey4", "limitKey5"};
        String columns[] = {"limitCol1", "limitCol2", "limitCol3"};
        String values[] = {"limitVal1", "limitVal2", "limitVal3", "limitVal4"};
        long tss[] = {ts, ts + 1, ts + 2, ts + 3};
        
        // 写入测试数据
        for (String key : keys) {
            Put put = new Put(toBytes(key));
            for (String column : columns) {
                for (int i = 0; i < values.length; i++) {
                    put.addColumn(family.getBytes(), column.getBytes(), tss[i], values[i].getBytes());
                }
            }
            hTable.put(put);
        }

        // 1. 测试setLimit 小于实际数据量的情况
        {
            Scan scan = new Scan(keys[0].getBytes(), "limitKey6".getBytes());
            scan.addFamily(family.getBytes());
            scan.setLimit(3); // 限制返回3个完整行
            ResultScanner scanner = hTable.getScanner(scan);
            List<Result> results = new ArrayList<>();
            for (Result result : scanner) {
                results.add(result);
            }
            // 预期：返回3个完整行
            Assert.assertTrue("返回行数应该不超过limit", results.size() == 3);
            
            // 校验每个返回行的数据正确性
            for (int i = 0; i < results.size(); i++) {
                Result result = results.get(i);
                String expectedKey = keys[i]; // 按顺序应该是limitKey1, limitKey2, limitKey3
                
                // 校验行键
                Assert.assertEquals("行键应该匹配", expectedKey, Bytes.toString(result.getRow()));
                
                // 校验每个行包含的列数据
                Cell[] cells = result.rawCells();
                Assert.assertEquals("每行应该包含" + (columns.length * values.length) + "个cell", 
                    columns.length * values.length, cells.length);
                
                // 按列名和时间戳排序校验
                sortCells(cells);
                int cellIdx = 0;
                for (String column : columns) {
                    for (int j = values.length - 1; j >= 0; j--) { // 按时间戳降序
                        AssertKeyValue(expectedKey, column, tss[j], values[j], cells[cellIdx++]);
                    }
                }
            }
        }

        // 2. 测试setLimit 大于实际数据量的情况
        {
            Scan scan = new Scan(keys[0].getBytes(), "limitKey6".getBytes());
            scan.addFamily(family.getBytes());
            scan.setLimit(10); // 限制返回10个完整行（大于实际数据量5行）
            ResultScanner scanner = hTable.getScanner(scan);
            List<Result> results = new ArrayList<>();
            for (Result result : scanner) {
                results.add(result);
            }
            // 预期：返回所有5个完整行
            Assert.assertEquals("应该返回所有数据", keys.length , results.size());
            
            // 校验每个返回行的数据正确性
            for (int i = 0; i < results.size(); i++) {
                Result result = results.get(i);
                String expectedKey = keys[i]; // 按顺序应该是limitKey1到limitKey5
                
                // 校验行键
                Assert.assertEquals("行键应该匹配", expectedKey, Bytes.toString(result.getRow()));
                
                // 校验每个行包含的列数据
                Cell[] cells = result.rawCells();
                Assert.assertEquals("每行应该包含" + (columns.length * values.length) + "个cell", 
                    columns.length * values.length, cells.length);
                
                // 按列名和时间戳排序校验
                sortCells(cells);
                int cellIdx = 0;
                for (String column : columns) {
                    for (int j = values.length - 1; j >= 0; j--) { // 按时间戳降序
                        AssertKeyValue(expectedKey, column, tss[j], values[j], cells[cellIdx++]);
                    }
                }
            }
        }

        // 3. 测试setLimit 为1的情况
        {
            Scan scan = new Scan(keys[0].getBytes(), "limitKey6".getBytes());
            scan.addFamily(family.getBytes());
            scan.setLimit(1); // 限制返回1个完整行
            ResultScanner scanner = hTable.getScanner(scan);
            List<Result> results = new ArrayList<>();
            for (Result result : scanner) {
                results.add(result);
            }
            // 预期：只返回1个完整行
            Assert.assertEquals("应该只返回1个完整行", 1, results.size());
            
            // 校验返回的唯一行的数据正确性
            Result result = results.get(0);
            String expectedKey = keys[0]; // 应该是limitKey1
            
            // 校验行键
            Assert.assertEquals("行键应该匹配", expectedKey, Bytes.toString(result.getRow()));
            
            // 校验该行包含的列数据
            Cell[] cells = result.rawCells();
            Assert.assertEquals("该行应该包含" + (columns.length * values.length) + "个cell", 
                columns.length * values.length, cells.length);
            
            // 按列名和时间戳排序校验
            sortCells(cells);
            int cellIdx = 0;
            for (String column : columns) {
                for (int j = values.length - 1; j >= 0; j--) { // 按时间戳降序
                    AssertKeyValue(expectedKey, column, tss[j], values[j], cells[cellIdx++]);
                }
            }
        }

        // 4. 测试指定单个列的setLimit
        {
            Scan scan = new Scan(keys[0].getBytes(), "limitKey6".getBytes());
            scan.addColumn(family.getBytes(), columns[0].getBytes());
            scan.setLimit(3); // 限制返回3个完整行
            ResultScanner scanner = hTable.getScanner(scan);
            List<Result> results = new ArrayList<>();
            for (Result result : scanner) {
                results.add(result);
            }
            // 预期：返回3个完整行
            Assert.assertTrue("返回行数应该不超过limit", results.size() == 3);
            
            // 校验每个返回行的单列数据正确性
            for (int i = 0; i < results.size(); i++) {
                Result result = results.get(i);
                String expectedKey = keys[i]; // 按顺序应该是limitKey1, limitKey2, limitKey3
                
                // 校验行键
                Assert.assertEquals("行键应该匹配", expectedKey, Bytes.toString(result.getRow()));
                
                // 校验该行只包含指定列的数据
                Cell[] cells = result.rawCells();
                Assert.assertEquals("该行应该只包含" + values.length + "个cell（单列多版本）", 
                    values.length, cells.length);
                
                // 按时间戳排序校验单列数据
                sortCells(cells);
                for (int j = 0; j < cells.length; j++) {
                    AssertKeyValue(expectedKey, columns[0], tss[values.length - 1 - j], 
                        values[values.length - 1 - j], cells[j]);
                }
            }
        }

        // 5. 测试时间范围 + setLimit
        {
            Scan scan = new Scan(keys[0].getBytes(), "limitKey6".getBytes());
            scan.addFamily(family.getBytes());
            scan.setTimeStamp(tss[2]); // 只查询特定时间戳的数据
            scan.setLimit(3); // 限制返回3个完整行
            ResultScanner scanner = hTable.getScanner(scan);
            List<Result> results = new ArrayList<>();
            for (Result result : scanner) {
                results.add(result);
            }
            // 预期：返回3个完整行，且都是指定时间戳的数据
            Assert.assertTrue("返回行数应该不超过limit", results.size() == 3);
            
            // 校验每个返回行的数据正确性
            for (int i = 0; i < results.size(); i++) {
                Result result = results.get(i);
                String expectedKey = keys[i]; // 按顺序应该是limitKey1, limitKey2, limitKey3
                
                // 校验行键
                Assert.assertEquals("行键应该匹配", expectedKey, Bytes.toString(result.getRow()));
                
                // 校验该行包含的列数据（只有指定时间戳的数据）
                Cell[] cells = result.rawCells();
                Assert.assertEquals("该行应该包含" + columns.length + "个cell（指定时间戳）", 
                    columns.length, cells.length);
                
                // 校验每个cell的时间戳和值
                for (Cell cell : cells) {
                    Assert.assertEquals("时间戳应该匹配", tss[2], cell.getTimestamp());
                    // 校验列名和值
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    Assert.assertTrue("列名应该在预期范围内", 
                        Arrays.asList(columns).contains(columnName));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    Assert.assertEquals("值应该匹配", values[2], value);
                }
            }
        }
        System.out.println("testScanWithLimit run successfully");
    }
}
