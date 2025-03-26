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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.*;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static com.alipay.oceanbase.hbase.util.TableTemplateManager.TIMESERIES_TABLES;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;


public class OHTableTimeSeriesGetTest {
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


    public static void testGetImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        // 0. prepare data
        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String[] columns = {"putColumn1", "putColumn2", "putColumn3"};
        String[] columns1 = {"putColumn1", "putColumn2"};
        String[] columns2 = {"putColumn3"};
        String[] values = {"version1", "version2"}; // each column have two versions
        long curTs = System.currentTimeMillis();
        long[] ts = {curTs, curTs+1}; // each column have two versions
        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j < columns1.length; j++) {
                Put put = new Put(toBytes(key));
                put.add(family.getBytes(), columns1[j].getBytes(), ts[i], toBytes(values[i]));
                hTable.put(put);
            }
        }

        Put put = new Put(toBytes(key));
        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j < columns2.length; j++) {
                put.add(family.getBytes(), columns2[j].getBytes(), ts[i], toBytes(values[i]));
            }
        }
        hTable.put(put);


        // 1. get specify column
        {
            int index = 0;
            Get get = new Get(key.getBytes());
            get.addColumn(family.getBytes(), columns[index].getBytes());
            Result r = hTable.get(get);
            Cell cells[] = r.rawCells();
            assertEquals(2, cells.length);
            sortCells(cells);
            for (int i = values.length - 1; i >= 0; i--) {
                AssertKeyValue(key, columns[index], ts[i], values[i], cells[values.length - 1-i]);
            }
        }

        // 2. get do not specify column
        {
            Get get = new Get(key.getBytes());
            get.addFamily(family.getBytes());
            Result result = hTable.get(get);
            Cell[] cells = result.rawCells();
            assertEquals(columns.length * values.length, cells.length);
            sortCells(cells);
            int idx = 0;
            for (int i = 0; i < columns.length; i++) {
                for (int j = values.length - 1; j >= 0; j--) {
                    ObHTableSecondaryPartUtil.AssertKeyValue(key, columns[i], ts[j], values[j], cells[idx]);
                    idx++;
                }
            }
        }

        // 3. get specify time range
        {
            Get get = new Get(key.getBytes());
            get.addFamily(family.getBytes());
            get.setTimeStamp(ts[1]);
            Result r = hTable.get(get);
            Cell cells[] = r.rawCells();
            assertEquals(columns.length, cells.length);
            sortCells(cells);
            for (int i = 0; i < columns.length; i++) {
                AssertKeyValue(key, columns[i], values[1],cells[i]);
            }
        }

        hTable.close();
    }

    @Test
    public void testGet() throws Throwable {
        FOR_EACH(tableNames, OHTableTimeSeriesGetTest::testGetImpl);
    }
}
