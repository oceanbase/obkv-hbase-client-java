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
import org.junit.*;

import java.util.*;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static com.alipay.oceanbase.hbase.util.TableTemplateManager.TIMESERIES_TABLES;
import static com.alipay.oceanbase.hbase.util.TableTemplateManager.TableType.SECONDARY_PARTITIONED_TIME_RANGE_KEY;
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
                    put.add(family.getBytes(), column.getBytes(), tss[i], values[i].getBytes());
                }
            }
            hTable.put(put);
        }

        for (String key : keys2) {
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
            ResultScanner scanner = hTable.getScanner(scan);
            Result result = null;
            int resultSize = (keys.length * columns.length * values.length) / batchSize;
            for (int i = 0; i < resultSize; i++) {
                result = scanner.next();
                Assert.assertEquals(2, result.size());
            }
            result = scanner.next();
            Assert.assertEquals(null, result);
        }

        // 7. scan using setAllowPartialResults/setAllowPartialResults
        {
            Scan scan = new Scan(keys[0].getBytes(), endKey.getBytes());
            scan.addFamily(family.getBytes());
            scan.setMaxResultSize(10);
            scan.setAllowPartialResults(true);
            ResultScanner scanner = hTable.getScanner(scan);
            int resultSize = keys.length * columns.length * values.length;
            for (int i = 0; i < resultSize; i++) {
                Result result = scanner.next();
                Assert.assertEquals(1, result.size());
            }
            Result result = scanner.next();
            Assert.assertEquals(null, result);
        }

        // 8. scan in reverse
        {
//            Scan scan = new Scan(keys[2].getBytes(), keys[0].getBytes());
//            scan.addFamily(family.getBytes());
//            scan.setReversed(true);
//            ResultScanner scanner = hTable.getScanner(scan);
//            List<Cell> cells = getCellsFromScanner(scanner);
//
//            int cellIndex = 0;
//            for (int i = 1; i >= 0; i--) {
//                for (String column : columns) {
//                    AssertKeyValue(keys[i], column, lastTs, latestValue, cells.get(cellIndex));
//                    cellIndex++;
//                }
//            }
//            assertEquals(columns.length * 2, cells.size());
        }
    }

    @Test
    public void testScan() throws Throwable {
        FOR_EACH(tableNames, OHTableTimeSeriesScanTest::testScanImpl);
    }
}
