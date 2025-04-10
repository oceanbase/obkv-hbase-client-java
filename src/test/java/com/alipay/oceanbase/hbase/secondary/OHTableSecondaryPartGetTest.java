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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.junit.*;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static com.alipay.oceanbase.hbase.util.TableTemplateManager.TableType.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;

public class OHTableSecondaryPartGetTest {
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

    public static void testGetImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        // 0. prepare data
        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String[] columns = { "putColumn1", "putColumn2", "putColumn3" };
        String[] values = { "version1", "version2" }; // each column have two versions
        long curTs = System.currentTimeMillis();
        long[] ts = { curTs, curTs + 1 }; // each column have two versions
        String latestValue = values[1];
        long lastTs = ts[1];
        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j < columns.length; j++) {
                Put put = new Put(toBytes(key));
                put.addColumn(family.getBytes(), columns[j].getBytes(), ts[i], toBytes(values[i]));
                hTable.put(put);
            }
        }

        // 1. get specify column
        {
            int index = 0;
            Get get = new Get(key.getBytes());
            get.addColumn(family.getBytes(), columns[index].getBytes());
            Result r = hTable.get(get);
            Assert.assertEquals(1, r.size());
            AssertKeyValue(key, columns[index], lastTs, latestValue, r.rawCells()[0]);
        }

        // 2. get do not specify column
        {
            Get get = new Get(key.getBytes());
            get.addFamily(family.getBytes());
            Result result = hTable.get(get);
            Cell[] cells = result.rawCells();
            assertEquals(columns.length, cells.length);
            for (int i = 0; i < columns.length; i++) {
                ObHTableSecondaryPartUtil.AssertKeyValue(key, columns[i], lastTs, latestValue,
                    cells[i]);
            }
        }

        // 3. get specify versions
        {
            int index = 0;
            Get get = new Get(key.getBytes());
            get.addColumn(family.getBytes(), columns[index].getBytes());
            get.setMaxVersions(2);
            Result r = hTable.get(get);
            Assert.assertEquals(2, r.size());
            AssertKeyValue(key, columns[index], ts[1], values[1], r.rawCells()[0]);
            AssertKeyValue(key, columns[index], ts[0], values[0], r.rawCells()[1]);
        }

        // 4. get specify time range
        {
            Get get = new Get(key.getBytes());
            get.addFamily(family.getBytes());
            get.setMaxVersions(2);
            get.setTimeStamp(ts[1]);
            Result r = hTable.get(get);
            Assert.assertEquals(columns.length, r.size());
            for (int i = 0; i < columns.length; i++) {
                AssertKeyValue(key, columns[i], values[1], r.rawCells()[i]);
            }
        }

        // 5. get specify filter
        {
            Get get = new Get(key.getBytes());
            get.addFamily(family.getBytes());
            get.setMaxVersions(2);
            ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(toBytes(values[0])));
            get.setFilter(valueFilter);
            Result r = hTable.get(get);
            Assert.assertEquals(columns.length, r.size());
            for (int i = 0; i < columns.length; i++) {
                AssertKeyValue(key, columns[i], values[0], r.rawCells()[i]);
            }
        }

        hTable.close();
    }

    public static void testMultiCFGetImpl(Map.Entry<String, List<String>> entry) throws Exception {

        // 0. prepare data
        String key = "putKey";
        String[] columns = { "putColumn1", "putColumn2", "putColumn3" };
        String groupName = getTableName(entry.getKey());
        String[] values = { "version1", "version2" }; // each column have two versions
        String latestValue = values[1];
        List<String> tableNames = entry.getValue();
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(groupName);
        long timestamp = System.currentTimeMillis();
        long[] ts = { timestamp, timestamp + 1 };
        long lastTs = ts[1];
        hTable.init();

        for (String tableName : tableNames) {
            String family = getColumnFamilyName(tableName);
            for (int i = 0; i < values.length; i++) {
                for (int j = 0; j < columns.length; j++) {
                    Put put = new Put(toBytes(key));
                    put.addColumn(family.getBytes(), columns[j].getBytes(), ts[i],
                        toBytes(values[i]));
                    hTable.put(put);
                }
            }
        }

        // 1. get specify column
        {
            int columnIndex = 0;
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                Get get = new Get(key.getBytes());
                get.addColumn(family.getBytes(), columns[columnIndex].getBytes());
                Result r = hTable.get(get);
                Assert.assertEquals(1, r.size());
                AssertKeyValue(key, columns[columnIndex], lastTs, latestValue, r.rawCells()[0]);
            }
        }

        // 2. get do not specify column
        {
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                Get get = new Get(key.getBytes());
                get.addFamily(family.getBytes());
                Result result = hTable.get(get);
                Cell[] cells = result.rawCells();
                assertEquals(columns.length, cells.length);
                for (int i = 0; i < columns.length; i++) {
                    ObHTableSecondaryPartUtil.AssertKeyValue(key, columns[i], lastTs, latestValue,
                        cells[i]);
                }
            }
        }

        // 3. get do not specify column family
        {
            Get get = new Get(key.getBytes());
            Result r = hTable.get(get);
            Assert.assertEquals(tableNames.size() * columns.length, r.size());
            int cur = 0;
            for (String tableName : tableNames) {
                for (int i = 0; i < columns.length; i++) {
                    AssertKeyValue(key, columns[i], lastTs, latestValue, r.rawCells()[cur]);
                    cur++;
                }
            }
        }

        // 4. get specify multi cf and column
        {
            int columnIndex = 0;
            Get get = new Get(key.getBytes());
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                get.addColumn(family.getBytes(), columns[columnIndex].getBytes());
            }
            Result r = hTable.get(get);
            Assert.assertEquals(tableNames.size(), r.rawCells().length);
            for (int i = 0; i < tableNames.size(); i++) {
                AssertKeyValue(key, columns[columnIndex], lastTs, latestValue, r.rawCells()[i]);
            }
        }

        // 5. get specify multi cf and versions
        {
            Get get = new Get(key.getBytes());
            get.setMaxVersions(2);
            Result r = hTable.get(get);
            Assert.assertEquals(tableNames.size() * columns.length * ts.length, r.size());
            int cur = 0;
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                for (int i = 0; i < columns.length; i++) {
                    for (int k = ts.length - 1; k >= 0; k--) {
                        AssertKeyValue(key, family, columns[i], ts[k], values[k], r.rawCells()[cur]);
                        cur++;
                    }
                }
            }
        }

        // 6. get specify multi cf and time range
        {
            Get get = new Get(key.getBytes());
            get.setMaxVersions(2);
            get.setTimeStamp(ts[1]);
            Result r = hTable.get(get);
            Assert.assertEquals(tableNames.size() * columns.length, r.size());
            int cur = 0;
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                for (int i = 0; i < columns.length; i++) {
                    AssertKeyValue(key, family, columns[i], ts[1], values[1], r.rawCells()[cur]);
                    cur++;
                }
            }
        }

        // 7. get specify multi cf and filter
        {
            Get get = new Get(key.getBytes());
            get.setMaxVersions(2);
            ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(toBytes(values[0])));
            get.setFilter(valueFilter);
            Result r = hTable.get(get);
            Assert.assertEquals(tableNames.size() * columns.length, r.size());
            int cur = 0;
            for (String tableName : tableNames) {
                String family = getColumnFamilyName(tableName);
                for (int i = 0; i < columns.length; i++) {
                    AssertKeyValue(key, family, columns[i], ts[0], values[0], r.rawCells()[cur]);
                    cur++;
                }
            }
        }
        hTable.close();
    }

    @Test
    public void testGet() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartGetTest::testGetImpl);
    }

    @Test
    public void testMultiCFGet() throws Throwable {
        FOR_EACH(group2tableNames, OHTableSecondaryPartGetTest::testMultiCFGetImpl);
    }
}
