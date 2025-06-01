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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.util.*;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_SYS_PASSWORD;
import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.ODP_MODE;
import static com.alipay.oceanbase.hbase.util.TableTemplateManager.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;


public class OHTableShareStorageTest {
    private static List<String>              tableNames       = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = new LinkedHashMap<>();

    @BeforeClass
    public static void before() throws Exception {
        openDistributedExecute();
        for (TableTemplateManager.TableType type : NORMAL_PARTITIONED_TABLES) {
            createTables(type, tableNames, group2tableNames, true);
        }
        for (TableTemplateManager.TableType type : NORMAL_PARTITIONED_TABLES) {
            alterTables(type, tableNames, group2tableNames, true);
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


    public static void testGetImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        // 0. prepare data - 100 key
        long recordCount = 100;
        String family = getColumnFamilyName(tableName);
        String key = "Key";
        String column = "Column";
        String value = "Value";
        long curTs = System.currentTimeMillis();
        for (int i = 0; i < recordCount; i++) {
            Put put = new Put(toBytes(key + i));
            put.add(family.getBytes(), (column + i).getBytes(), curTs, toBytes(value + i));
            hTable.put(put);
        }

        // 1. get, expect less than 100 key
        long getCount = 0;
        for (int i = 0; i < recordCount; i++) {
            Get get = new Get((key + i).getBytes());
            get.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            get.addColumn(family.getBytes(), (column + i).getBytes());
            Result r = hTable.get(get);
            Cell cells[] = r.rawCells();
            getCount += cells.length;
        }
        System.out.println("getCount:" + getCount);
        Assert.assertTrue(getCount < recordCount);

        hTable.close();
    }

    public static void testMultiCFGetImpl(Map.Entry<String, List<String>> entry) throws Exception {
        // 0. prepare data
        long recordCount = 100;
        String groupName = getTableName(entry.getKey());
        List<String> tableNames = entry.getValue();
        String key = "Key";
        String column = "Column";
        String value = "Value";
        long curTs = System.currentTimeMillis();
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(groupName);
        hTable.init();

        for (String tableName : tableNames) {
            String family = getColumnFamilyName(tableName);
            for (int i = 0; i < recordCount; i++) {
                Put put = new Put(toBytes(key + i));
                put.add(family.getBytes(), (column + i).getBytes(), curTs, toBytes(value + i));
                hTable.put(put);
            }
        }

        // 1. get specify column
        {
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                long getCount = 0;
                for (int i = 0; i < recordCount; i++) {
                    Get get = new Get((key + i).getBytes());
                    get.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
                    get.addColumn(family.getBytes(), (column + i).getBytes());
                    Result r = hTable.get(get);
                    Cell cells[] = r.rawCells();
                    getCount += cells.length;
                    System.out.println("getCount:" + getCount);
                }
                Assert.assertTrue(getCount < recordCount);
            }
        }

        // 2. get do not specify column
        {
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                long getCount = 0;
                for (int i = 0; i < recordCount; i++) {
                    Get get = new Get((key + i).getBytes());
                    get.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
                    get.addFamily(family.getBytes());
                    Result r = hTable.get(get);
                    Cell cells[] = r.rawCells();
                    getCount += cells.length;
                    System.out.println("getCount:" + getCount);
                }
                Assert.assertTrue(getCount < recordCount);
            }
        }

        // 3. get do not specify column family
        {
            long getCount = 0;
            Get get = new Get(key.getBytes());
            get.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            Result r = hTable.get(get);
            Cell cells[] = r.rawCells();
            getCount += cells.length;
            System.out.println("getCount:" + getCount);
            Assert.assertTrue(getCount < recordCount);
        }

        // 4. get specify multi cf and column
        {
            long getCount = 0;
            Get get = new Get(key.getBytes());
            get.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                get.addColumn(family.getBytes(), column.getBytes());
            }
            Result r = hTable.get(get);
            Cell cells[] = r.rawCells();
            getCount += cells.length;
            System.out.println("getCount:" + getCount);
            Assert.assertTrue(getCount < recordCount);
        }

        hTable.close();
    }

    public static void testBatchGetImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        // 0. prepare data - 100 key
        long recordCount = 100;
        String family = getColumnFamilyName(tableName);
        String key = "Key";
        String column = "Column";
        String value = "Value";
        long curTs = System.currentTimeMillis();
        for (int i = 0; i < recordCount; i++) {
            Put put = new Put(toBytes(key + i));
            put.add(family.getBytes(), (column + i).getBytes(), curTs, toBytes(value));
            hTable.put(put);
        }

        List<Get> gets = new ArrayList<>();
        // 1. get, expect less than 100 key
        long getCount = 0;
        for (int i = 0; i < recordCount; i++) {
            String getKey = null;
            if (i == 4) {
                getKey = key + 101;
            } else {
                getKey = key + i;
            }
            Get get = new Get((getKey + i).getBytes());
            get.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            get.addColumn(family.getBytes(), (column + i).getBytes());
            gets.add(get);
        }
        Result[] results = hTable.get(gets);
        for (Result result : results) {
            getCount += result.size();
        }
        Assert.assertTrue(getCount < recordCount);

        hTable.close();
    }

    public static void testScanImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        // 0. prepare data - 100 key
        long recordCount = 100;
        String family = getColumnFamilyName(tableName);
        String key = "Key";
        String column = "Column";
        String value = "Value";
        long curTs = System.currentTimeMillis();
        for (int i = 0; i < recordCount; i++) {
            Put put = new Put(toBytes(key + i));
            put.add(family.getBytes(), (column).getBytes(), curTs, toBytes(value + i));
            hTable.put(put);
        }

        // 1. scan not specify column
        {
            Scan scan = new Scan(key.getBytes(), (key+recordCount).getBytes());
            scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            scan.addFamily(family.getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    count++;
                }
            }
            Assert.assertTrue(count < recordCount);
        }

        // 2. scan specify column
        {
            Scan scan = new Scan(key.getBytes(), (key+recordCount).getBytes());
            scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            scan.addColumn(family.getBytes(), column.getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    count++;
                }
            }
            Assert.assertTrue(count < recordCount);
        }

        // 3. scan specify versions
        {
            Scan scan = new Scan(key.getBytes(), (key+recordCount).getBytes());
            scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            scan.setMaxVersions(2);
            scan.addColumn(family.getBytes(), column.getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    count++;
                }
            }
            Assert.assertTrue(count < recordCount);
        }


        // 4. scan specify filter
        {
            Scan scan = new Scan(key.getBytes(), (key+recordCount).getBytes());
            scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            scan.setMaxVersions(2);
            scan.addFamily(family.getBytes());
            ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(toBytes(value)));
            scan.setFilter(valueFilter);
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    count++;
                }
            }
            Assert.assertTrue(count < recordCount);
        }

        // 5. scan in reverse
        {
            Scan scan = new Scan(key.getBytes(), (key+recordCount).getBytes());
            scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            scan.addFamily(family.getBytes());
            scan.setReversed(true);
            try {
                ResultScanner scanner = hTable.getScanner(scan);
                int count = 0;
                for (Result result : scanner) {
                    for (Cell cell : result.rawCells()) {
                        count++;
                    }
                }
                Assert.assertTrue(count < recordCount);
            } catch (Exception e) {
                Assert.assertTrue(e.getCause().getMessage().contains("secondary partitioned hbase table with reverse query not supported"));
            }
        }
        hTable.close();
    }

    public static void testMultiCFScanImpl(Map.Entry<String, List<String>> entry) throws Exception {
        String groupName = getTableName(entry.getKey());
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(groupName);
        hTable.init();

        // 0. prepare data - 100 key
        long recordCount = 100;
        String key = "Key";
        String column = "Column";
        String value = "Value";
        long curTs = System.currentTimeMillis();
        List<String> tableNames = entry.getValue();

        for (String tableName : tableNames) {
            String family = getColumnFamilyName(tableName);
            for (int i = 0; i < recordCount; i++) {
                Put put = new Put(toBytes(key + i));
                put.add(family.getBytes(), (column).getBytes(), curTs, toBytes(value));
                hTable.put(put);
            }
        }

        // 1. multi cf scan specify one cf and one column
        {
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                Scan scan = new Scan(key.getBytes(), (key + recordCount).getBytes());
                scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
                scan.addColumn(family.getBytes(), column.getBytes());
                ResultScanner scanner = hTable.getScanner(scan);
                int count = 0;
                for (Result result : scanner) {
                    for (Cell cell : result.rawCells()) {
                        count++;
                    }
                }
                Assert.assertTrue(count < recordCount);
            }
        }

        // 2. multi cf scan specify one cf without specify column
        {
            for (String tableName : tableNames) {
                Scan scan = new Scan(key.getBytes(), (key + recordCount).getBytes());
                scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
                String family = getColumnFamilyName(tableName);
                scan.addFamily(family.getBytes());
                ResultScanner scanner = hTable.getScanner(scan);
                int count = 0;
                for (Result result : scanner) {
                    for (Cell cell : result.rawCells()) {
                        count++;
                    }
                }
                Assert.assertTrue(count < recordCount);
            }
        }

        // 3. multi cf scan do not specify cf
        {
            Scan scan = new Scan(key.getBytes(), (key + recordCount).getBytes());
            scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    count++;
                }
            }
            Assert.assertTrue(count < recordCount);
        }

        // 4. multi cf scan specify multi cf and multi column
        {
            Scan scan = new Scan(key.getBytes(), (key + recordCount).getBytes());
            scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                scan.addColumn(family.getBytes(), column.getBytes());
            }
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    count++;
                }
            }
            Assert.assertTrue(count < recordCount);
        }

        // 5. multi cf scan specify versions
        {
            Scan scan = new Scan(key.getBytes(), (key + recordCount).getBytes());
            scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            scan.setMaxVersions(2);
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    count++;
                }
            }
            Assert.assertTrue(count < recordCount);
        }

        // 6. multi cf scan specify time range
        {
            Scan scan = new Scan(key.getBytes(), (key + recordCount).getBytes());
            scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            scan.setMaxVersions(2);
            scan.setTimeStamp(curTs);
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    count++;
                }
            }
            Assert.assertTrue(count < recordCount);
        }

        // 7. multi cf scan specify filter
        {
            Scan scan = new Scan(key.getBytes(), (key + recordCount).getBytes());
            scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            scan.setMaxVersions(2);
            ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(toBytes(value)));
            scan.setFilter(valueFilter);
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    count++;
                }
            }
            Assert.assertTrue(count < recordCount);
        }

        // 8. multi cf scan using setStartRow/setEndRow
        {
            Scan scan = new Scan();
            scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            scan.setStartRow(key.getBytes());
            scan.setStopRow((key + recordCount).getBytes());
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    count++;
                }
            }
            Assert.assertTrue(count < recordCount);
        }

        // 9. multi cf scan using batch
        {
            Scan scan = new Scan(key.getBytes(), (key + recordCount).getBytes());
            scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            scan.setBatch(2);
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    count++;
                }
            }
            Assert.assertTrue(count < recordCount);
        }

        // 10. multi cf scan with family scan and column-specific scan
        {
            Scan scan = new Scan(key.getBytes(), (key + recordCount).getBytes());
            scan.setAttribute(HBASE_HTABLE_QUERY_HOT_ONLY, "true".getBytes());
            for (int i = 0; i < tableNames.size(); i++) {
                String family = getColumnFamilyName(tableNames.get(i));
                if (i % 2 == 0) {
                    scan.addFamily(family.getBytes());
                } else {
                    scan.addColumn(family.getBytes(), column.getBytes());
                }
            }
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    count++;
                }
            }
            Assert.assertTrue(count < recordCount);
        }
        hTable.close();
    }

    @Test
    public void testGet() throws Throwable {
        FOR_EACH(tableNames, OHTableShareStorageTest::testGetImpl);
    }

    @Test
    public void testMultiCFGet() throws Throwable {
        FOR_EACH(group2tableNames, OHTableShareStorageTest::testMultiCFGetImpl);
    }

    @Test
    public void testBatchGet() throws Throwable {
        FOR_EACH(tableNames, OHTableShareStorageTest::testBatchGetImpl);
    }

    @Test
    public void testScan() throws Throwable {
        FOR_EACH(tableNames, OHTableShareStorageTest::testScanImpl);
    }

    @Test
    public void testMultiCFScan() throws Throwable {
        FOR_EACH(group2tableNames, OHTableShareStorageTest::testMultiCFScanImpl);
    }
}
