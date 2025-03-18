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
    private static List<String> tableNames       = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = new LinkedHashMap<>();

    @BeforeClass
    public static void before() throws Exception {
        openDistributedExecute();
        for (TableTemplateManager.TableType type : TableTemplateManager.TableType.values()) {
            if (!type.name().contains("TIME")) {
                createTables(type, tableNames, group2tableNames, true);
                alterTableTimeToLive(tableNames, true, 10);
                for (List<String> groupTableNames : group2tableNames.values()) {
                    alterTableTimeToLive(groupTableNames, true, 10);
                }
            }
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

    public static void testTTLImpl(List<String> tableNames) throws Exception {
        closeTTLExecute();
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
                        put.add(family.getBytes(), column.getBytes(), values[i].getBytes());
                        hTable.put(put);
                    }
                }
            }
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
        }

        // 3. using sql to scan expired but not delete yet hbase data
        for (String tableName : tableNames) {
            Assert.assertEquals(keys.length * columns.length * values.length,
                    getSQLTableRowCnt(tableName));
        }

        // 4. open ttl knob to delete expired hbase data
        openTTLExecute();

        // 5. check util expired hbase data is deleted by ttl tasks
        checkUtilTimeout(()-> {
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
        closeTTLExecute();
    }

    public static void testMultiCFTTLImpl(Map<String, List<String>> group2tableNames) throws Exception {
        closeTTLExecute();
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
                            put.add(family.getBytes(), column.getBytes(), values[i].getBytes());
                            hTable.put(put);
                        }
                    }
                }
            }

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
        }

        // 3. using sql to scan expired but not delete yet hbase data
        for (String tableName : allTableNames) {
            Assert.assertEquals(keys.length * columns.length * values.length,
                    getSQLTableRowCnt(tableName));
        }

        // 4. open ttl knob to delete expired hbase data
        openTTLExecute();

        // 5. check util expired hbase data is deleted by ttl tasks
        checkUtilTimeout(()-> {
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
        closeTTLExecute();
    }


    @Test
    public void testTTL() throws Throwable {
      testTTLImpl(tableNames);
    }

    @Test
    public void testMultiCFTTL() throws Throwable {
        testMultiCFTTLImpl(group2tableNames);
    }
}
