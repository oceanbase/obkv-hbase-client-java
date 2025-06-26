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
import com.mysql.cj.exceptions.AssertionFailedException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.*;

import java.util.*;

import static com.alipay.oceanbase.hbase.util.OHTableHotkeyThrottleUtil.OperationType.get;
import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.TableTemplateManager.TIMESERIES_TABLES;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OHTableTimeSeriesDeleteTest {
    private static List<String> tableNames       = new LinkedList<String>();
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
    
    public static void testDeleteColumnImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        
        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String[] columns = {"putColumn1", "putColumn2"};
        long curTs = System.currentTimeMillis();
        long[] ts = {curTs, curTs+1}; // each column have two versions
        String[] values = {"version1", "version2"};
        
        for (int i = 0; i < values.length; i++) {
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), columns[i].getBytes(), ts[i], toBytes(values[i]));
            hTable.put(put);
        }

 
        
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), columns[0].getBytes());
        get.addColumn(family.getBytes(), columns[1].getBytes());
        Result result = hTable.get(get);
        {
            final Cell[] cells = result.rawCells();
            Assert(tableName, () -> Assert.assertEquals(cells.length, 2));
            sortCells(cells);
            for (int i = 0; i < values.length; i++) {
                AssertKeyValue(key, columns[i], ts[i], values[i], cells[i]);
            }
        }
        {
            Delete delete = new Delete(toBytes(key));
            delete.addColumn(family.getBytes(), columns[0].getBytes());
            hTable.delete(delete);
            result = hTable.get(get);
            final Cell[] cells = result.rawCells();
            Assert(tableName, () -> Assert.assertEquals(cells.length, 1));
            AssertKeyValue(key, columns[1], ts[1], values[1], cells[0]);
        }

        {
            Delete delete = new Delete(toBytes(key));
            delete.addColumn(family.getBytes(), columns[1].getBytes());
            hTable.delete(delete);
            result = hTable.get(get);
            final Cell[] cells = result.rawCells();
            Assert(tableName, () -> Assert.assertEquals(cells.length, 0));
        }
    }
    
    public static void testDeleteColumnsImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String[] columns = {"putColumn1", "putColumn2"};
        long curTs = System.currentTimeMillis();
        long[] ts = {curTs, curTs+1}; // each column have two versions
        String[] values = {"version1", "version2"};

        for (int i = 0; i < values.length; i++) {
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), columns[i].getBytes(), ts[i] + i, toBytes(values[i] + i));
            hTable.put(put);
        }



        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), columns[0].getBytes());
        get.addColumn(family.getBytes(), columns[1].getBytes());
        Result result = hTable.get(get);
        {
            final Cell[] cells = result.rawCells();
            Assert(tableName, () -> Assert.assertEquals(cells.length, 2));
            sortCells(cells);
            for (int i = 0; i < values.length; i++) {
                AssertKeyValue(key, columns[i], ts[i] + i, values[i] + i, cells[i]);
            }
        }
        {
            Delete delete = new Delete(toBytes(key));
            delete.addColumns(family.getBytes(), columns[0].getBytes());
            hTable.delete(delete);
            result = hTable.get(get);
            final Cell[] cells = result.rawCells();
            Assert(tableName, () -> Assert.assertEquals(cells.length, 1));
            AssertKeyValue(key, columns[1], ts[1] + 1, values[1] + 1, cells[0]);
        }

        {
            Delete delete = new Delete(toBytes(key));
            delete.addColumns(family.getBytes(), columns[1].getBytes());
            hTable.delete(delete);
            result = hTable.get(get);
            final Cell[] cells = result.rawCells();
            Assert(tableName, () -> Assert.assertEquals(cells.length, 0));
        }
    }
    
    public static void testDeleteFamilyImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String[] columns = {"putColumn1", "putColumn2"};
        long curTs = System.currentTimeMillis();
        long[] ts = {curTs, curTs+1}; // each column have two versions
        String[] values = {"version1", "version2"};

        for (int i = 0; i < values.length; i++) {
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), columns[i].getBytes(), ts[i] + i, toBytes(values[i] + i));
            hTable.put(put);
        }



        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), columns[0].getBytes());
        get.addColumn(family.getBytes(), columns[1].getBytes());
        Result result = hTable.get(get);
        {
            final Cell[] cells = result.rawCells();
            Assert(tableName, () -> Assert.assertEquals(cells.length, 2));
            sortCells(cells);
            for (int i = 0; i < values.length; i++) {
                AssertKeyValue(key, columns[i], ts[i] + i, values[i] + i, cells[i]);
            }
        }
        {
            Delete delete = new Delete(toBytes(key));
            delete.addFamily(family.getBytes());
            hTable.delete(delete);
            result = hTable.get(get);
            final Cell[] cells = result.rawCells();
            Assert(tableName, () -> Assert.assertEquals(cells.length, 0));
        }
        {
            Delete delete = new Delete(toBytes(key));
            delete.addFamily(family.getBytes());
            delete.addFamily("notExistFamily".getBytes());
            try {
                hTable.delete(delete);
                fail();
            } catch (Exception e) {
                assertTrue(e.getCause().getMessage().contains("timeseries hbase table with multi column family not supported"));
            }
        }
    }
    
    public static void testDeleteFamilyWithTimestampImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String[] columns = {"putColumn1", "putColumn2"};
        long curTs = System.currentTimeMillis();

        String value = "value";

        for (int i = 0; i < 3; i++) {
            Put put = new Put(toBytes(key));
            put.addColumn(family.getBytes(), columns[0].getBytes(), curTs + i, toBytes(value + i));
            put.addColumn(family.getBytes(), columns[1].getBytes(), curTs + i, toBytes(value + i));
            hTable.put(put);
        }


        {
            Delete delete = new Delete(toBytes(key));
            delete.addFamily(family.getBytes(), curTs + 1);
            hTable.delete(delete);
            Get get = new Get(toBytes(key));
            get.addColumn(family.getBytes(), columns[0].getBytes());
            get.addColumn(family.getBytes(), columns[1].getBytes());
            Result result = hTable.get(get);
            final Cell[] cells = result.rawCells();
            Assert(tableName, () -> Assert.assertEquals(cells.length, 2));
            sortCells(cells);
            AssertKeyValue(key, columns[0], curTs + 2, value + 2, cells[0]);
            AssertKeyValue(key, columns[1], curTs + 2, value + 2, cells[1]);
        }
    }
    
    public static void testDeleteFamilyVersionImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String[] columns = {"putColumn1", "putColumn2"};
        long curTs = System.currentTimeMillis();
        
        String value = "value";

        for (int i = 0; i < 3; i++) {
            Put put = new Put(toBytes(key));
            put.add(family.getBytes(), columns[0].getBytes(), curTs + i, toBytes(value + i));
            put.add(family.getBytes(), columns[1].getBytes(), curTs + i, toBytes(value + i));
            hTable.put(put);
        }
        

        {
            Delete delete = new Delete(toBytes(key));
            delete.addFamilyVersion(family.getBytes(), curTs + 1);
            hTable.delete(delete);
            Get get = new Get(toBytes(key));
            get.addColumn(family.getBytes(), columns[0].getBytes());
            get.addColumn(family.getBytes(), columns[1].getBytes());
            Result result = hTable.get(get);
            final Cell[] cells = result.rawCells();
            Assert(tableName, () -> Assert.assertEquals(cells.length, 2));
            sortCells(cells);
            AssertKeyValue(key, columns[0], curTs + 2, value + 2, cells[0]);
            AssertKeyValue(key, columns[1], curTs + 2, value + 2, cells[1]);
        }
    }
    
    @Test
    public void testDeleteColumn() throws Exception {
        for (String tableName : tableNames) {
            testDeleteColumnImpl(tableName);
        }
    }
    
    @Test
    public void testDeleteColumns() throws Exception {
        for (String tableName : tableNames) {
            testDeleteColumnsImpl(tableName);
        }
    }
    
    @Test
    public void testDeleteFamily() throws Exception {
        for (String tableName : tableNames) {
            testDeleteFamilyImpl(tableName);
        }
    }
    
    @Test
    public void testDeleteFamilyWithTimestamp() throws Exception {
        for (String tableName : tableNames) {
            testDeleteFamilyWithTimestampImpl(tableName);
        }
    }

    @Test
    public void testDeleteFamilyVersion() throws Exception {
        for (String tableName : tableNames) {
            testDeleteFamilyVersionImpl(tableName);
        }
    }
}
