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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.junit.*;

import java.util.*;

import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_QUERY_HOT_ONLY;
import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static com.alipay.oceanbase.hbase.util.TableTemplateManager.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;


public class OHTableShareStorageSeriesTest {
    private static List<String>              tableNames       = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = new LinkedHashMap<>();


    @BeforeClass
    public static void before() throws Exception {
        openDistributedExecute();
        for (TableType type : NORMAL_SERIES_PARTITIONED_TABLES) {
            createTables(type, tableNames, group2tableNames, true);
        }
        for (TableType type : NORMAL_SERIES_PARTITIONED_TABLES) {
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

        // 4. scan specify time range
        {
            Scan scan = new Scan(key.getBytes(), (key+recordCount).getBytes());
            scan.setMaxVersions(2);
            scan.setTimeStamp(curTs);
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

        hTable.close();
    }

    @Test
    public void testGet() throws Throwable {
        FOR_EACH(tableNames, OHTableShareStorageSeriesTest::testGetImpl);
    }


    @Test
    public void testScan() throws Throwable {
        FOR_EACH(tableNames, OHTableShareStorageSeriesTest::testScanImpl);
    }

}
