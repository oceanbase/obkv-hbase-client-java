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
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.junit.*;

import java.io.IOException;
import java.util.*;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.*;

public class OHTableSecondaryPartScanTest {
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

        String keys[] = { "putKey1", "putKey2", "putKey3" };
        String columns[] = { "putColumn1", "putColumn2" };
        String values[] = { "putValue1", "putValue2" };
        long tss[] = { ts, ts + 1 };
        long lastTs = tss[1];
        String latestValue = values[1];

        for (String key : keys) {
            for (String column : columns) {
                for (int i = 0; i < values.length; i++) {
                    Put put = new Put(toBytes(key));
                    put.add(family.getBytes(), column.getBytes(), tss[i], values[i].getBytes());
                    hTable.put(put);
                }
            }
        }

        // 1. scan specify column
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            scan.addColumn(family.getBytes(), columns[0].getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    AssertKeyValue(keys[count], columns[0], lastTs, latestValue, cell);
                    count++;
                }
            }
            assertEquals(2, count);
        }

        // 2. scan do not specify column
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            scan.addFamily(family.getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);

            int cellIndex = 0;
            for (int i = 0; i < 2; i++) {
                for (String column : columns) {
                    AssertKeyValue(keys[i], column, lastTs, latestValue, cells.get(cellIndex));
                    cellIndex++;
                }
            }
            assertEquals(columns.length * 2, cells.size());
        }

        // 3. scan specify versions
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            scan.setMaxVersions(2);
            scan.addColumn(family.getBytes(), columns[0].getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);

            assertEquals(tss.length * 2, cells.size());
            int cellIndex = 0;
            for (int i = 0; i < 2; i++) {
                for (int k = tss.length - 1; k >= 0; k--) {
                    AssertKeyValue(keys[i], columns[0], tss[k], values[k], cells.get(cellIndex));
                    cellIndex++;
                }
            }
        }

        // 4. scan specify time range
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            scan.setMaxVersions(2);
            scan.setTimeStamp(tss[1]);
            scan.addFamily(family.getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);
            assertEquals(columns.length * 2, cells.size());
            int cellIndex = 0;
            for (int i = 0; i < 2; i++) {
                for (String column : columns) {
                    AssertKeyValue(keys[i], column, lastTs, latestValue, cells.get(cellIndex));
                    cellIndex++;
                }
            }
        }

        // 5. scan specify filter
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            scan.setMaxVersions(2);
            scan.addFamily(family.getBytes());
            ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(toBytes(values[0])));
            scan.setFilter(valueFilter);
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);
            Assert.assertEquals(columns.length * 2, cells.size());
            int cellIndex = 0;
            for (int i = 0; i < 2; i++) {
                for (String column : columns) {
                    AssertKeyValue(keys[i], column, values[0], cells.get(cellIndex));
                    cellIndex++;
                }
            }
        }

        // 6. scan using setStartRow/setEndRow
        {
            Scan scan = new Scan();
            scan.setStartRow(keys[0].getBytes());
            scan.setStopRow(keys[2].getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);

            int cellIndex = 0;
            for (int i = 0; i < 2; i++) {
                for (String column : columns) {
                    AssertKeyValue(keys[i], column, lastTs, latestValue, cells.get(cellIndex));
                    cellIndex++;
                }
            }
            assertEquals(columns.length * 2, cells.size());
        }

        // 7. scan using batch
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            scan.addFamily(family.getBytes());
            scan.setBatch(2);
            ResultScanner scanner = hTable.getScanner(scan);
            Result result = scanner.next();
            Assert.assertEquals(2, result.size());
            result = scanner.next();
            Assert.assertEquals(2, result.size());
            result = scanner.next();
            Assert.assertEquals(null, result);
        }

        // 7. scan using setAllowPartialResults/setAllowPartialResults
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            scan.addFamily(family.getBytes());
            scan.setMaxResultSize(10);
            scan.setAllowPartialResults(true);
            ResultScanner scanner = hTable.getScanner(scan);
            for (int i = 0; i < 4; i++) {
                Result result = scanner.next();
                Assert.assertEquals(1, result.size());
            }
            Result result = scanner.next();
            Assert.assertEquals(null, result);
        }
        
        hTable.close();
    }
    
    public static void testReverseScanImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        // 0. prepare data
        String family = getColumnFamilyName(tableName);
        long ts = System.currentTimeMillis();
        String keys[] = { "putKey1", "putKey2", "putKey3", "putKey4", "putKey5" };
        String columns[] = { "putColumn1", "putColumn2" };
        for (String key : keys) {
            for (String column : columns) {
                Put put = new Put(toBytes(key));
                put.add(family.getBytes(), column.getBytes(), ts, toBytes("value" + key + column));
                hTable.put(put);
            }
        }
        
        // 1. scan in reverse
        Scan scan = new Scan();
        scan.setReversed(true);
        if (tableName.contains("secondary")) {
            try {
                ResultScanner scanner = hTable.getScanner(scan);
                fail("unexpected, should failed before");
            } catch (Exception e) {
                assertTrue(e.getCause().getMessage().contains("secondary partitioned hbase table with reverse query not supported"));
            }
        } else {
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);
            Assert.assertEquals(keys.length * columns.length, cells.size());
            // check value
            System.out.println(cells);
            int cellidx = 0;
            for (int i = keys.length - 1; i >= 0; i --) {
                for (String column : columns) {
                    AssertKeyValue(keys[i], family, column, ts, "value" + keys[i] + column, cells.get(cellidx++));
                }
            }
        }
    }

    public static void testMultiCFScanImpl(Map.Entry<String, List<String>> entry) throws Exception {
        // 0. prepare data
        // putKey1 putColumn1 putValue1,ts
        // putKey1 putColumn1 putValue2,ts+1
        // putKey1 putColumn2 putValue1,ts
        // putKey1 putColumn2 putValue2,ts+1
        // ...
        String groupName = getTableName(entry.getKey());
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(groupName);
        hTable.init();
        long ts = System.currentTimeMillis();

        String keys[] = { "putKey1", "putKey2", "putKey3" };
        String columns[] = { "putColumn1", "putColumn2" };
        String values[] = { "putValue1", "putValue2" };
        long tss[] = { ts, ts + 1 };
        long lastTs = tss[1];
        String latestValue = values[1];
        List<String> tableNames = entry.getValue();

        for (String tableName : tableNames) {
            String family = getColumnFamilyName(tableName);
            for (String key : keys) {
                for (String column : columns) {
                    for (int i = 0; i < values.length; i++) {
                        Put put = new Put(toBytes(key));
                        put.add(family.getBytes(), column.getBytes(), tss[i], values[i].getBytes());
                        hTable.put(put);
                    }
                }
            }
        }

        // 1. multi cf scan specify one cf and one column
        {
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
                scan.addColumn(family.getBytes(), columns[0].getBytes());
                ResultScanner scanner = hTable.getScanner(scan);
                List<Cell> cells = getCellsFromScanner(scanner);
                Assert.assertEquals(2, cells.size());
                for (int i = 0; i < 2; i++) {
                    AssertKeyValue(keys[i], family, columns[0], lastTs, latestValue, cells.get(i));
                }
            }
        }

        // 2. multi cf scan specify one cf without specify column
        {
            for (String tableName : tableNames) {
                Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
                String family = getColumnFamilyName(tableName);
                scan.addFamily(family.getBytes());
                ResultScanner scanner = hTable.getScanner(scan);
                List<Cell> cells = getCellsFromScanner(scanner);

                int cellIndex = 0;
                for (int i = 0; i < 2; i++) {
                    for (String column : columns) {
                        AssertKeyValue(keys[i], family, column, lastTs, latestValue,
                            cells.get(cellIndex));
                        cellIndex++;
                    }
                }
                assertEquals(columns.length * 2, cells.size());
            }
        }

        // 3. multi cf scan do not specify cf
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);

            assertEquals(tableNames.size() * 2 * columns.length, cells.size());
            int cellIndex = 0;
            for (int i = 0; i < 2; i++) {
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    for (String column : columns) {
                        Cell cell = cells.get(cellIndex);
                        AssertKeyValue(keys[i], family, column, lastTs, latestValue,
                            cells.get(cellIndex));
                        cellIndex++;
                    }
                }
            }
        }

        // 4. multi cf scan specify multi cf and multi column
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                for (String column : columns) {
                    scan.addColumn(family.getBytes(), column.getBytes());
                }
            }
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);

            assertEquals(tableNames.size() * 2 * columns.length, cells.size());
            int cellIndex = 0;
            for (int i = 0; i < 2; i++) {
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    for (String column : columns) {
                        AssertKeyValue(keys[i], family, column, lastTs, latestValue,
                            cells.get(cellIndex));
                        cellIndex++;
                    }
                }
            }
        }

        // 5. multi cf scan specify versions
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            scan.setMaxVersions(2);
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);

            assertEquals(tableNames.size() * 2 * columns.length * tss.length, cells.size());
            int cellIndex = 0;
            for (int i = 0; i < 2; i++) {
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    for (String column : columns) {
                        for (int j = values.length - 1; j >= 0; j--) {
                            AssertKeyValue(keys[i], family, column, tss[j], values[j],
                                cells.get(cellIndex));
                            cellIndex++;
                        }
                    }
                }
            }
        }

        // 6. multi cf scan specify time range
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            scan.setMaxVersions(2);
            scan.setTimeStamp(tss[1]);
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);
            assertEquals(tableNames.size() * columns.length * 2, cells.size());
            int cellIndex = 0;
            for (int i = 0; i < 2; i++) {
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    for (String column : columns) {
                        AssertKeyValue(keys[i], family, column, lastTs, latestValue,
                            cells.get(cellIndex));
                        cellIndex++;
                    }
                }
            }
        }

        // 7. multi cf scan specify filter
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            scan.setMaxVersions(2);
            ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(toBytes(values[0])));
            scan.setFilter(valueFilter);
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);
            Assert.assertEquals(tableNames.size() * columns.length * 2, cells.size());
            int cellIndex = 0;
            for (int i = 0; i < 2; i++) {
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    for (String column : columns) {
                        AssertKeyValue(keys[i], family, column, tss[0], values[0],
                            cells.get(cellIndex));
                        cellIndex++;
                    }
                }
            }
        }

        // 8. multi cf scan using setStartRow/setEndRow
        {
            Scan scan = new Scan();
            scan.setStartRow(keys[0].getBytes());
            scan.setStopRow(keys[2].getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);
            assertEquals(tableNames.size() * columns.length * 2, cells.size());

            int cellIndex = 0;
            for (int i = 0; i < 2; i++) {
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    for (String column : columns) {
                        AssertKeyValue(keys[i], family, column, lastTs, latestValue,
                            cells.get(cellIndex));
                        cellIndex++;
                    }
                }
            }
        }

        // 9. multi cf scan using batch
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            scan.setBatch(2);
            ResultScanner scanner = hTable.getScanner(scan);
            Result result = null;
            for (String tableName : tableNames) {
                result = scanner.next();
                Assert.assertEquals(2, result.size());
                result = scanner.next();
                Assert.assertEquals(2, result.size());
            }
            result = scanner.next();
            Assert.assertEquals(null, result);
        }

        // 10. multi cf scan with family scan and column-specific scan
        {
            Scan scan = new Scan(keys[0].getBytes(), keys[2].getBytes());
            for (int i = 0; i < tableNames.size(); i++) {
                String family = getColumnFamilyName(tableNames.get(i));
                if (i % 2 == 0) {
                    scan.addFamily(family.getBytes());
                } else {
                    for (String column : columns) {
                        scan.addColumn(family.getBytes(), column.getBytes());
                    }
                }
            }
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);
            assertEquals(tableNames.size() * 2 * columns.length, cells.size());
            int cellIndex = 0;
            for (int i = 0; i < 2; i++) {
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    for (String column : columns) {
                        AssertKeyValue(keys[i], family, column, lastTs, latestValue,
                            cells.get(cellIndex));
                        cellIndex++;
                    }
                }
            }
        }
        hTable.close();
    }
    
    public static void testMultiCFReverseScanImpl(Map.Entry<String, List<String>> entry) throws Exception {
        String groupName = getTableName(entry.getKey());
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(groupName);
        hTable.init();
        long ts = System.currentTimeMillis();

        String keys[] = { "putKey1", "putKey2", "putKey3", "putKey4", "putKey5" };
        String columns[] = { "putColumn1", "putColumn2" };
        String values[] = { "putValue1", "putValue2" };
        long tss[] = { ts, ts + 1 };
        long lastTs = tss[1];
        String latestValue = values[1];
        List<String> tableNames = entry.getValue();
        
        for (String key : keys) {
            Put put = new Put(toBytes(key));
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                for (String column : columns) {
                    put.addColumn(toBytes(family), toBytes(column), ts, toBytes("value" + key + family + column));
                }
            }
            hTable.put(put);
        }
        
        if (groupName.contains("secondary")) {
            try {
                Scan scan = new Scan();
                scan.setReversed(true);
                ResultScanner scanner = hTable.getScanner(scan);
                fail("unexpected, should failed before");
            } catch (IOException e) {
                assertTrue(e.getCause().getMessage().contains("secondary partitioned hbase table with reverse query not supported"));
            }
        } else {
            Scan scan = new Scan();
            scan.setReversed(true);
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);
            assertEquals(keys.length * 2 * tableNames.size(), cells.size());
            System.out.println(cells);
            int cellidx = 0;
            for (int i = keys.length - 1; i >= 0; i --) {
                for (String tableName : tableNames) {
                    String family = getColumnFamilyName(tableName);
                    for (String column : columns) {
                        AssertKeyValue(keys[i], family, column, ts, "value" + keys[i] + family + column, cells.get(cellidx++));
                    }
                }
            }
        }
        
    }

    @Test
    public void testScan() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartScanTest::testScanImpl);
    }

    @Test
    public void testReverseScan() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartScanTest::testReverseScanImpl);
    }
    
    @Test
    public void testMultiCFScan() throws Throwable {
        FOR_EACH(group2tableNames, OHTableSecondaryPartScanTest::testMultiCFScanImpl);
    }
    
    @Test
    public void testMiltiCFReverseScan() throws Throwable {
        FOR_EACH(group2tableNames, OHTableSecondaryPartScanTest::testMultiCFReverseScanImpl);
    }
}
