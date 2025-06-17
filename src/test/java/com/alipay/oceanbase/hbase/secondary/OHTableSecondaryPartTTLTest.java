/*-
 * #%L
 * com.oceanbase:obkv-hbase-client
 * %%
 * Copyright (C) 2022 - 2025 OceanBase Group
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
import java.util.stream.Collectors;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static java.lang.Thread.sleep;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;

public class OHTableSecondaryPartTTLTest {
    private static List<String>              tableNames       = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = new LinkedHashMap<String, List<String>>();

    @BeforeClass
    public static void before() throws Exception {
        openDistributedExecute();
        for (TableTemplateManager.TableType type : TableTemplateManager.NORMAL_TABLES) {
            createTables(type, tableNames, group2tableNames, true);
        }
        alterTableTimeToLive(tableNames, true, 10);
        for (List<String> groupTableNames : group2tableNames.values()) {
            alterTableTimeToLive(groupTableNames, true, 10);
        }
    }

    @AfterClass
    public static void finish() throws Exception {
        dropTables(tableNames, group2tableNames);
        closeDistributedExecute();
    }

    @Before
    public void prepareCase() throws Exception {
        truncateTables(tableNames, group2tableNames);
    }

    public static void testTTLImpl(List<String> tableNames) throws Exception {
        disableTTL();
        // 0. prepare data
        String keys[] = {"putKey1", "putKey2", "putKey3"};
        String endKey = "putKey4";
        String columns[] = {"putColumn1", "putColumn2"};
        String values[] = {"putValue1", "putValue2", "putValue3", "putValue4"};
        for (String tableName : tableNames) {
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
            hTable.init();
            String family = getColumnFamilyName(tableName);
            for (String key : keys) {
                for (String column : columns) {
                    for (int i = 0; i < values.length; i++) {
                        Put put = new Put(toBytes(key));
                        put.addColumn(family.getBytes(), column.getBytes(), values[i].getBytes());
                        hTable.put(put);
                    }
                }
            }
            hTable.close();
        }

        // 1. sleep util data expired
        sleep(12000);

        // 2. hbase scan/get cannot not return expired data
        for (String tableName : tableNames) {
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
            hTable.init();
            String family = getColumnFamilyName(tableName);
            Scan scan = new Scan(keys[0].getBytes(), endKey.getBytes());
            scan.addFamily(family.getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);
            assertEquals(0, cells.size());

            Get get = new Get(keys[0].getBytes());
            get.addFamily(family.getBytes());
            Result result = hTable.get(get);
            Assert.assertEquals(0, result.rawCells().length);
            hTable.close();
        }

        // 3. using sql to scan expired but not delete yet hbase data
        for (String tableName : tableNames) {
            Assert.assertEquals(keys.length * columns.length * values.length,
                    getSQLTableRowCnt(tableName));
        }

        // 4. open ttl knob to delete expired hbase data
        enableTTL();
        triggerTTL();

        // 5. check util expired hbase data is deleted by ttl tasks
        checkUtilTimeout(tableNames, ()-> {
            try {
                return getRunningNormalTTLTaskCnt() == 0;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, 50000, 3000);

        for (String tableName : tableNames) {
            Assert.assertEquals(0, getSQLTableRowCnt(tableName));
        }

        // 6. close ttl knob
        disableTTL();
    }

    public static void testMultiCFTTLImpl(Map<String, List<String>> group2tableNames) throws Exception {
        disableTTL();
        List<String> allTableNames = group2tableNames.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        // 0. prepare data
        String keys[] = {"putKey1", "putKey2", "putKey3"};
        String endKey = "putKey4";
        String columns[] = {"putColumn1", "putColumn2"};
        String values[] = {"putValue1", "putValue2", "putValue3", "putValue4"};

        for (Map.Entry<String, List<String>> entry : group2tableNames.entrySet()) {
            String groupName = getTableName(entry.getKey());
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(groupName);
            hTable.init();
            List<String> tableNames = entry.getValue();
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                for (String key : keys) {
                    for (String column : columns) {
                        for (int i = 0; i < values.length; i++) {
                            Put put = new Put(toBytes(key));
                            put.addColumn(family.getBytes(), column.getBytes(), values[i].getBytes());
                            hTable.put(put);
                        }
                    }
                }
            }
            hTable.close();
        }

        // 1. sleep util data expired
        sleep(12000);

        // 2. hbase multi cf scan cannot not return expired data
        for (Map.Entry<String, List<String>> entry : group2tableNames.entrySet()) {
            String groupName = getTableName(entry.getKey());
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(groupName);
            hTable.init();
            Scan scan = new Scan(keys[0].getBytes(), endKey.getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(scanner);
            assertEquals(0, cells.size());

            Get get = new Get(keys[0].getBytes());
            Result result = hTable.get(get);
            Assert.assertEquals(0, result.rawCells().length);
            hTable.close();
        }

        // 3. using sql to scan expired but not delete yet hbase data
        for (String tableName : allTableNames) {
            Assert.assertEquals(keys.length * columns.length * values.length,
                    getSQLTableRowCnt(tableName));
        }

        // 4. open ttl knob to delete expired hbase data
        enableTTL();
        triggerTTL();

        // 5. check util expired hbase data is deleted by ttl tasks
        checkUtilTimeout(allTableNames, ()-> {
            try {
                return getRunningNormalTTLTaskCnt() == 0;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, 50000, 3000);

        for (String tableName : allTableNames) {
            Assert.assertEquals(0, getSQLTableRowCnt(tableName));
        }

        // 6. close ttl knob
        disableTTL();
    }

    void testRowkeyTTL(List<String> tableNames, Boolean useScan, Boolean isReversed) throws Exception {
        disableTTL();
        // 0. prepare data
        String keys[] = {"putKey1", "putKey2", "putKey3"};
        String endKey = "putKey4";
        String reversedEndKey = "putKey";
        String columns[] = {"putColumn1", "putColumn2"};
        String values[] = {"putValue1", "putValue2", "putValue3", "putValue4"};
        for (String tableName : tableNames) {
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
            hTable.init();
            String family = getColumnFamilyName(tableName);
            for (String key : keys) {
                for (String column : columns) {
                    for (int i = 0; i < values.length; i++) {
                        Put put = new Put(toBytes(key));
                        put.addColumn(family.getBytes(), column.getBytes(),  values[i].getBytes());
                        hTable.put(put);
                    }
                }
            }
            hTable.close();
        }

        // 1. sleep util data expired
        sleep(12000);

        // 2. enable kv ttl
        enableTTL();

        // 3. SQL can scan expired but not delete yet hbase data
        for (String tableName : tableNames) {
            ObHTableTestUtil.Assert(tableName, ()-> {
                try {
                    Assert.assertEquals(keys.length * columns.length * values.length,
                            getSQLTableRowCnt(tableName));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // 4. use Hbase scan/get expired data to trigger ttl
        for (String tableName : tableNames) {
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
            hTable.init();
            String family = getColumnFamilyName(tableName);
            if (useScan) {
                Scan scan = new Scan();
                if (isReversed) {
                    scan.setReversed(true);
                    scan.setStartRow(keys[2].getBytes());
                    scan.setStopRow(reversedEndKey.getBytes());
                } else {
                    scan.setStartRow(keys[0].getBytes());
                    scan.setStopRow(endKey.getBytes());
                }
                scan.addFamily(family.getBytes());
                ResultScanner scanner = hTable.getScanner(scan);
                List<Cell> cells = getCellsFromScanner(scanner);
                assertEquals(0, cells.size());
            } else {
                for (String key : keys) {
                    Get get = new Get(key.getBytes());
                    get.addFamily(family.getBytes());
                    Result result = hTable.get(get);
                    assertEquals(0, result.rawCells().length);
                }
            }
            hTable.close();
        }

        // 5. wait to disable
        checkUtilTimeout(tableNames, ()-> {
            try {
                Boolean passed = true;
                for (int i = 0; passed && i < tableNames.size(); i++) {
                    if (getSQLTableRowCnt(tableNames.get(i)) > 0) {
                        passed = false;
                    }
                }
                return passed;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, 50000, 3000);

        // 6. disable ttl
        disableTTL();
    }

    void testMultiCFRowkeyTTL(Map<String, List<String>> group2tableNames, Boolean useScan, Boolean isReversed) throws Exception {
        disableTTL();
        List<String> allTableNames = group2tableNames.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        // 0. prepare data
        String keys[] = {"putKey1", "putKey2", "putKey3"};
        String endKey = "putKey4";
        String columns[] = {"putColumn1", "putColumn2"};
        String values[] = {"putValue1", "putValue2", "putValue3", "putValue4"};
        for (Map.Entry<String, List<String>> entry : group2tableNames.entrySet()) {
            String groupName = getTableName(entry.getKey());
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(groupName);
            hTable.init();
            for (String tableName : entry.getValue()) {
                String family = getColumnFamilyName(tableName);
                for (String key : keys) {
                    for (String column : columns) {
                        for (int i = 0; i < values.length; i++) {
                            Put put = new Put(toBytes(key));
                            put.addColumn(family.getBytes(), column.getBytes(), values[i].getBytes());
                            hTable.put(put);
                        }
                    }
                }
            }
            hTable.close();
        }

        // 1. sleep util data expired
        sleep(12000);

        // 2. enable kv ttl
        enableTTL();

        // 3. SQL can scan expired but not delete yet hbase data
        for (String tableName : allTableNames) {
            Assert.assertEquals(keys.length * columns.length * values.length,
                    getSQLTableRowCnt(tableName));
        }

        // 4. use Hbase scan expired data to trigger ttl
        for (Map.Entry<String, List<String>> entry : group2tableNames.entrySet()) {
            String groupName = getTableName(entry.getKey());
            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(groupName);
            hTable.init();
            if (useScan) {
                Scan scan = new Scan(keys[0].getBytes(), endKey.getBytes());
                scan.setReversed(isReversed);
                ResultScanner scanner = hTable.getScanner(scan);
                List<Cell> cells = getCellsFromScanner(scanner);
                assertEquals(0, cells.size());
            } else {
                for (String key : keys) {
                    Get get = new Get(key.getBytes());
                    Result result = hTable.get(get);
                    assertEquals(0, result.rawCells().length);
                }
            }
            hTable.close();
        }

        // 5. wait to disable
        checkUtilTimeout(allTableNames, ()-> {
            try {
                Boolean passed = true;
                for (int i = 0; passed && i < allTableNames.size(); i++) {
                    if (getSQLTableRowCnt(allTableNames.get(i)) > 0) {
                        passed = false;
                    }
                }
                return passed;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, 70000, 5000);

        // 6. disable ttl
        disableTTL();
    }

    @Test
    public void testTTL() throws Throwable {
        testTTLImpl(tableNames);
    }

    @Test
    public void testMultiCFTTL() throws Throwable {
        testMultiCFTTLImpl(group2tableNames);
    }

    @Test
    public void testRowkeyTTL() throws Exception {
        testRowkeyTTL(tableNames, true, false);
        testRowkeyTTL(tableNames, false, false);
        // TODO: open the test after reverse scan is ok
        //         testRowkeyTTL(tableNames, true, true);
    }

    @Test
    public void testMultiCFRowkeyTTL() throws Exception {
        testMultiCFRowkeyTTL(group2tableNames, true, false);
        testMultiCFRowkeyTTL(group2tableNames, false, false);
        // TODO: open the test after reverse scan is ok
        // testMultiCFRowkeyTTL(group2tableNames, true, true);
    }
}
