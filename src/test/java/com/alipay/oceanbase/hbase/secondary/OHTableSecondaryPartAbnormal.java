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
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static com.alipay.oceanbase.hbase.util.TableTemplateManager.SERIES_TABLES;
import static com.alipay.oceanbase.hbase.util.TableTemplateManager.TableType.*;
import static org.junit.Assert.*;

public class OHTableSecondaryPartAbnormal {
    private static List<String>              tableNames       = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = new LinkedHashMap<String, List<String>>();
    private static byte[]                    ROW              = Bytes.toBytes("testRow");
    private static byte[]                    QUALIFIER        = Bytes.toBytes("testQualifier");
    private static byte[]                    VALUE_2          = Bytes.toBytes("abcd");

    @BeforeClass
    public static void before() throws Exception {
        openDistributedExecute();
        for (TableTemplateManager.TableType type : SERIES_TABLES) {
            createTables(type, tableNames, group2tableNames, true);
        }
    }

    @AfterClass
    public static void finish() throws Exception {
        closeDistributedExecute();
        //        dropTables(tableNames, group2tableNames);
    }

    @Before
    public void prepareCase() throws Exception {
        truncateTables(tableNames, group2tableNames);
    }

    private static void testSeriesLimit(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        byte[] FAMILY = getColumnFamilyName(tableName).getBytes();
        Put put2 = new Put(ROW);
        put2.add(FAMILY, QUALIFIER, VALUE_2);
        hTable.put(put2);
        Get get = new Get(ROW);
        get.addFamily(FAMILY);
        get.setFilter(new PageFilter(10));
        try {
            hTable.get(get);
            fail("unexpected, should failed before");
        } catch (IOException e) {
            assertTrue(e.getCause().getMessage()
                .contains("timeseries hbase table with filter query not supported"));
        }
        get = new Get(ROW);
        get.addFamily(FAMILY);
        get.setCheckExistenceOnly(true);
        try {
            hTable.get(get);
            fail("unexpected, should failed before");
        } catch (IOException e) {
            assertTrue(e.getCause().getMessage()
                .contains("timeseries hbase table with check existence only query not supported"));
        }
        get = new Get(ROW);
        get.addFamily(FAMILY);
        get.setClosestRowBefore(true);
        try {
            hTable.get(get);
            fail("unexpected, should failed before");
        } catch (IOException e) {
            assertTrue(e.getCause().getMessage()
                .contains("timeseries hbase table with reverse query not supported"));
        }
        Scan scan = new Scan(ROW);
        scan.addFamily(FAMILY);
        scan.setReversed(true);
        try {
            hTable.getScanner(scan);
            fail("unexpected, should failed before");
        } catch (IOException e) {
            assertTrue(e.getCause().getMessage()
                .contains("timeseries hbase table with reverse query not supported"));
        }
        scan = new Scan(ROW);
        scan.addFamily(FAMILY);
        scan.setCaching(1);
        try {
            hTable.getScanner(scan);
            fail("unexpected, should failed before");
        } catch (IOException e) {
            assertTrue(e.getCause().getMessage()
                .contains("timeseries hbase table with caching query not supported"));
        }
        scan = new Scan(ROW);
        scan.addFamily(FAMILY);
        scan.setBatch(1);
        try {
            hTable.getScanner(scan);
            fail("unexpected, should failed before");
        } catch (IOException e) {
            assertTrue(e.getCause().getMessage()
                .contains("timeseries hbase table with batch query not supported"));
        }
        scan = new Scan(ROW);
        scan.addFamily(FAMILY);
        scan.setAllowPartialResults(true);
        try {
            hTable.getScanner(scan);
            fail("unexpected, should failed before");
        } catch (IOException e) {
            assertTrue(e.getCause().getMessage()
                .contains("timeseries hbase table with allow partial results query not supported"));
        }
        scan = new Scan(ROW);
        scan.addFamily(FAMILY);
        scan.setMaxResultsPerColumnFamily(1);
        try {
            hTable.getScanner(scan);
            fail("unexpected, should failed before");
        } catch (IOException e) {
            assertTrue(e
                .getCause()
                .getMessage()
                .contains(
                    "timeseries hbase table with max results per column family query not supported"));
        }
        scan = new Scan(ROW);
        scan.addFamily(FAMILY);
        scan.setRowOffsetPerColumnFamily(1);
        try {
            hTable.getScanner(scan);
            fail("unexpected, should failed before");
        } catch (IOException e) {
            assertTrue(e.getCause().getMessage()
                .contains("timeseries hbase table with row offset query not supported"));
        }
        scan = new Scan(ROW);
        scan.addFamily(FAMILY);
        ResultScanner scanner = hTable.getScanner(scan);
        assertEquals(1, scanner.next().size());
        hTable.close();
    }

    private static void testSeriesWithCellTTL(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        byte[] FAMILY = getColumnFamilyName(tableName).getBytes();
        Put put2 = new Put(ROW);
        put2.add(FAMILY, QUALIFIER, VALUE_2);
        try {
            hTable.put(put2);
            fail("unexpected, should failed before");
        } catch (IOException e) {
            assertTrue(e.getCause().getMessage()
                .contains("series table not support cell ttl not supported"));
        }
        hTable.close();
    }

    private static void testSecondaryPartReverseScan(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        byte[] FAMILY = getColumnFamilyName(tableName).getBytes();
        Put put2 = new Put(ROW);
        put2.add(FAMILY, QUALIFIER, VALUE_2);
        hTable.put(put2);
        Scan scan = new Scan(ROW);
        scan.addFamily(FAMILY);
        scan.setReversed(true);
        try {
            hTable.getScanners(scan);
            fail("unexpected, should failed before");
        } catch (IOException e) {
            assertTrue(e.getCause().getMessage()
                .contains("secondary partitioned hbase table with reverse query not supported"));
        }
        hTable.close();
    }

    @Test
    public void testSeriesLimit() throws Throwable {
        FOR_EACH(tableNames, com.alipay.oceanbase.hbase.secondary.OHTableSecondaryPartAbnormal::testSeriesLimit);
    }

    @Test
    public void testSeriesWithCellTTL() throws Throwable {
        List<String> tmpTable = new ArrayList<>();
        createTables(NON_PARTITIONED_TIME_CELL_TTL, tmpTable, null, true);
        FOR_EACH(tmpTable, OHTableSecondaryPartAbnormal::testSeriesWithCellTTL);
        dropTables(tmpTable, null);
    }

    @Test
    public void testSecondaryPartReverseScan() throws Throwable {
        List<String> tmpTable = new ArrayList<>();
        createTables(SECONDARY_PARTITIONED_KEY_RANGE_GEN, tmpTable, null, true);
        FOR_EACH(tmpTable, OHTableSecondaryPartAbnormal::testSecondaryPartReverseScan);
        dropTables(tmpTable, null);
    }
}
