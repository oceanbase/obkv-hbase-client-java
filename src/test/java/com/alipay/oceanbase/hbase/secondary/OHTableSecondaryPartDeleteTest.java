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
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.*;

import java.util.*;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHTableSecondaryPartDeleteTest {
    private static List<String> tableNames = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = new LinkedHashMap<>();


    @BeforeClass
    public static void before() throws Exception {
        openDistributedExecute();
        for (TableTemplateManager.TableType type : TableTemplateManager.TableType.values()) {
            createTables(type, tableNames, group2tableNames, true);
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
    
    public static void testDeleteLastVersionImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column = "putColumn";
        String value = "value";

        Long ts1 = System.currentTimeMillis();
        Long ts2 = ts1 + 1000;
        Long ts3 = ts2 + 1000;
        Long ts4 = ts3 + 1000;
        { // delete last version
            Put put = new Put(toBytes(key));
            put.add(toBytes(family), toBytes(column), ts1, toBytes(value));
            put.add(toBytes(family), toBytes(column), ts2, toBytes(value));
            put.add(toBytes(family), toBytes(column), ts3, toBytes(value));
            put.add(toBytes(family), toBytes(column), ts4, toBytes(value));
//            hTable.put(put);

            Delete delete = new Delete(toBytes(key));
            delete.deleteColumn(toBytes(family), toBytes(column));
            hTable.delete(delete);

            Get get = new Get(toBytes(key));
            get.addColumn(toBytes(family), toBytes(column));
            get.setMaxVersions();
            Result result = hTable.get(get);
            Assert.assertEquals(3, result.size()); // ts4 is deleted
            Assert.assertEquals(value, result.getValue(family.getBytes(), column.getBytes()));
            for (Cell cell : result.rawCells()) {
                Assert.assertTrue(cell.getTimestamp() != ts4);
            }
        }
    }
    
    public static void testDeleteSpecifiedImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column = "putColumn";
        String value = "value";

        Long ts1 = System.currentTimeMillis();
        Long ts2 = ts1 + 1000;
        Long ts3 = ts2 + 1000;
        Long ts4 = ts3 + 1000;

        {
            Put put = new Put(toBytes(key));
            put.add(toBytes(family), toBytes(column), ts1, toBytes(value + ts1));
            put.add(toBytes(family), toBytes(column), ts2, toBytes(value + ts2));
            put.add(toBytes(family), toBytes(column), ts3, toBytes(value + ts3));
            put.add(toBytes(family), toBytes(column), ts4, toBytes(value + ts4));
            hTable.put(put);

            Delete delete = new Delete(toBytes(key));
            delete.addColumn(toBytes(family), toBytes(column), ts1);
            hTable.delete(delete);

            Get get = new Get(toBytes(key));
            get.addColumn(toBytes(family), toBytes(column));
            get.setMaxVersions();
            Result result = hTable.get(get);
            Assert(tableName, ()->Assert.assertEquals(3, result.size())); // ts1 is deleted
            Assert(tableName, ()->Assert.assertTrue(secureCompare((value + ts4).getBytes(), result.getValue(family.getBytes(), column.getBytes()))));
            for (Cell cell : result.rawCells()) {
                Assert(tableName, ()->Assert.assertTrue(cell.getTimestamp() != ts1));
            }
        }
    }
    
    public static void testDeleteColumnImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column = "putColumn";
        String value = "value";

        Long ts1 = System.currentTimeMillis();
        Long ts2 = ts1 + 1000;
        Long ts3 = ts2 + 1000;
        Long ts4 = ts3 + 1000;

        {
            Put put = new Put(toBytes(key));
            put.add(toBytes(family), toBytes(column), ts1, toBytes(value + ts1));
            put.add(toBytes(family), toBytes(column), ts2, toBytes(value + ts2));
            put.add(toBytes(family), toBytes(column), ts3, toBytes(value + ts3));
            put.add(toBytes(family), toBytes(column), ts4, toBytes(value + ts4));
            hTable.put(put);

            Delete delete = new Delete(toBytes(key));
            delete.addColumns(toBytes(family), toBytes(column), ts3);
            hTable.delete(delete);

            Get get = new Get(toBytes(key));
            get.addColumn(toBytes(family), toBytes(column));
            get.setMaxVersions();
            Result result = hTable.get(get);
            Assert.assertEquals(1, result.size()); // ts1, ts2, ts3 is deleted
            Assert.assertEquals(Arrays.toString((value + ts4).getBytes()), Arrays.toString(result.getValue(family.getBytes(), column.getBytes())));
            for (Cell cell : result.rawCells()) {
                Assert.assertTrue(cell.getTimestamp() != ts1);
            }
        }
    }
    
    public static void testDeleteFamilyImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column = "putColumn";
        String value = "value";

        Long ts1 = System.currentTimeMillis();
        Long ts2 = ts1 + 1000;
        Long ts3 = ts2 + 1000;
        Long ts4 = ts3 + 1000;

        {
            Put put = new Put(toBytes(key));
            put.add(toBytes(family), toBytes(column), ts1, toBytes(value + ts1));
            put.add(toBytes(family), toBytes(column), ts2, toBytes(value + ts2));
            put.add(toBytes(family), toBytes(column), ts3, toBytes(value + ts3));
            put.add(toBytes(family), toBytes(column), ts4, toBytes(value + ts4));
            hTable.put(put);

            Delete delete = new Delete(toBytes(key));
            delete.addFamily(toBytes(family));
            hTable.delete(delete);

            Get get = new Get(toBytes(key));
            get.addColumn(toBytes(family), toBytes(column));
            get.setMaxVersions();
            Result result = hTable.get(get);
            Assert.assertEquals(0, result.size());
        }
    }
    
    public static void testDeleteFamilyVersionImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column = "putColumn";
        String value = "value";

        Long ts1 = System.currentTimeMillis();
        Long ts2 = ts1 + 1000;
        Long ts3 = ts2 + 1000;
        Long ts4 = ts3 + 1000;

        {
            Put put = new Put(toBytes(key));
            put.add(toBytes(family), toBytes(column), ts1, toBytes(value + ts1));
            put.add(toBytes(family), toBytes(column), ts2, toBytes(value + ts2));
            put.add(toBytes(family), toBytes(column), ts3, toBytes(value + ts3));
            put.add(toBytes(family), toBytes(column), ts4, toBytes(value + ts4));
            hTable.put(put);

            Delete delete = new Delete(toBytes(key));
            delete.addFamily(toBytes(family), ts2);
            hTable.delete(delete);

            Get get = new Get(toBytes(key));
            get.addColumn(toBytes(family), toBytes(column));
            get.setMaxVersions();
            Result result = hTable.get(get);
            Assert.assertEquals(2, result.size());
            for (Cell cell : result.rawCells()) {
                Assert.assertTrue(cell.getTimestamp() != ts1);
                Assert.assertTrue(cell.getTimestamp() != ts2);
            }
        }
    }

    public static void testMultiCFDeleteLastVersionImpl(Map.Entry<String, List<String>> entry) throws Exception {
        String key = "putKey";
        String value = "value";
        String column = "putColumn";

        long ts1 = System.currentTimeMillis();
        long ts2 = ts1 + 1000;
        long ts3 = ts2 + 1000;
        long ts4 = ts3 + 1000;
        
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(entry.getKey());
        hTable.init();
        Put put = new Put(toBytes(key));
        Delete delete = new Delete(toBytes(key));
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            put.addColumn(toBytes(family), toBytes(column + family), ts1, toBytes(value + ts1));
            put.addColumn(toBytes(family), toBytes(column + family), ts2, toBytes(value + ts2));
            put.addColumn(toBytes(family), toBytes(column + family), ts3, toBytes(value + ts3));
            put.addColumn(toBytes(family), toBytes(column + family), ts4, toBytes(value + ts4));
            delete.addColumn(toBytes(family), toBytes(column + family));
        }
        hTable.put(put);
        hTable.delete(delete);
        
        
        Get get = new Get(toBytes(key));
        get.setMaxVersions();
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            get.addColumn(toBytes(family), toBytes(column + family));
        }
        Result result = hTable.get(get);

        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            // last version ts4 is deleted, so the last version is ts3
            Assert(tableName, ()->Assert.assertTrue(secureCompare((value + ts3).getBytes(),
                                                                        result.getValue(family.getBytes(), (column + family).getBytes()))));
            for (Cell cell : result.rawCells()) {
                Assert(tableName, ()->Assert.assertTrue("should not found last version ts4", cell.getTimestamp() != ts4));
            }
        }
    }

    public static void testMultiCFDeleteSpecifiedImpl(Map.Entry<String, List<String>> entry) throws Exception {
        String key = "putKey";
        String value = "value";
        String column = "putColumn";

        long ts1 = System.currentTimeMillis();
        long ts2 = ts1 + 1000;
        long ts3 = ts2 + 1000;
        long ts4 = ts3 + 1000;

        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(entry.getKey());
        hTable.init();
        Put put = new Put(toBytes(key));
        Delete delete = new Delete(toBytes(key));
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            put.addColumn(toBytes(family), toBytes(column + family), ts1, toBytes(value + ts1));
            put.addColumn(toBytes(family), toBytes(column + family), ts2, toBytes(value + ts2));
            put.addColumn(toBytes(family), toBytes(column + family), ts3, toBytes(value + ts3));
            put.addColumn(toBytes(family), toBytes(column + family), ts4, toBytes(value + ts4));
            delete.addColumn(toBytes(family), toBytes(column + family), ts2);
        }
        hTable.put(put);
        hTable.delete(delete);


        Get get = new Get(toBytes(key));
        get.setMaxVersions();
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            get.addColumn(toBytes(family), toBytes(column + family));
        }
        Result result = hTable.get(get);

        for (String tableName : entry.getValue()) {
            for (Cell cell : result.rawCells()) {
                Assert(tableName, ()->Assert.assertTrue("should not found last version ts2",cell.getTimestamp() != ts2));
            }
        }
    }
    
    public static void testMultiCFDeleteColumnImpl(Map.Entry<String, List<String>> entry) throws Exception {
        String key = "putKey";
        String value = "value";
        String column = "putColumn";

        long ts1 = System.currentTimeMillis();
        long ts2 = ts1 + 1000;
        long ts3 = ts2 + 1000;
        long ts4 = ts3 + 1000;

        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(entry.getKey());
        hTable.init();
        Put put = new Put(toBytes(key));
        Delete delete = new Delete(toBytes(key));
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            put.addColumn(toBytes(family), toBytes(column + family), ts1, toBytes(value + ts1));
            put.addColumn(toBytes(family), toBytes(column + family), ts2, toBytes(value + ts2));
            put.addColumn(toBytes(family), toBytes(column + family), ts3, toBytes(value + ts3));
            put.addColumn(toBytes(family), toBytes(column + family), ts4, toBytes(value + ts4));
            delete.addColumns(toBytes(family), toBytes(column + family), ts2);
        }
        hTable.put(put);
        hTable.delete(delete);


        Get get = new Get(toBytes(key));
        get.setMaxVersions();
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            get.addColumn(toBytes(family), toBytes(column + family));
        }
        Result result = hTable.get(get);

        for (String tableName : entry.getValue()) {
            for (Cell cell : result.rawCells()) {
                Assert(tableName, ()->Assert.assertTrue("should not found last version ts2",cell.getTimestamp() != ts2));
            }
        }
    }
    
    
    public static void testMultiCFDeleteFamilyImpl(Map.Entry<String, List<String>> entry) throws Exception {
        String key = "putKey";
        String value = "value";
        String column = "putColumn";

        long ts1 = System.currentTimeMillis();
        long ts2 = ts1 + 1000;
        long ts3 = ts2 + 1000;
        long ts4 = ts3 + 1000;

        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(entry.getKey());
        hTable.init();
        Put put = new Put(toBytes(key));
        Delete delete = new Delete(toBytes(key));
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            put.addColumn(toBytes(family), toBytes(column + family), ts1, toBytes(value + ts1));
            put.addColumn(toBytes(family), toBytes(column + family), ts2, toBytes(value + ts2));
            put.addColumn(toBytes(family), toBytes(column + family), ts3, toBytes(value + ts3));
            put.addColumn(toBytes(family), toBytes(column + family), ts4, toBytes(value + ts4));
            delete.addFamily(toBytes(family));
        }
        hTable.put(put);
        hTable.delete(delete);


        Get get = new Get(toBytes(key));
        get.setMaxVersions();
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            get.addColumn(toBytes(family), toBytes(column + family));
        }
        Result result = hTable.get(get);
        Assert.assertEquals(0, result.size());
    }
    
    public static void testMultiCFDeleteFamilyVersionImpl(Map.Entry<String, List<String>> entry) throws Exception {
        String key = "putKey";
        String value = "value";
        String column = "putColumn";

        long ts1 = System.currentTimeMillis();
        long ts2 = ts1 + 1000;
        long ts3 = ts2 + 1000;
        long ts4 = ts3 + 1000;

        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(entry.getKey());
        hTable.init();
        Put put = new Put(toBytes(key));
        Delete delete = new Delete(toBytes(key));
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            put.addColumn(toBytes(family), toBytes(column + family), ts1, toBytes(value + ts1));
            put.addColumn(toBytes(family), toBytes(column + family), ts2, toBytes(value + ts2));
            put.addColumn(toBytes(family), toBytes(column + family), ts3, toBytes(value + ts3));
            put.addColumn(toBytes(family), toBytes(column + family), ts4, toBytes(value + ts4));
            delete.addFamily(toBytes(family), ts2);
        }
        hTable.put(put);
        hTable.delete(delete);


        Get get = new Get(toBytes(key));
        get.setMaxVersions();
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            get.addColumn(toBytes(family), toBytes(column + family));
        }
        Result result = hTable.get(get);
        Assert.assertEquals(2 * 3, result.size());
    }
    
    public static void testDeleteAllImpl(Map.Entry<String, List<String>> entry) throws Exception {
        String key = "putKey";
        String value = "value";
        String column = "putColumn";

        long ts1 = System.currentTimeMillis();
        long ts2 = ts1 + 1000;
        long ts3 = ts2 + 1000;
        long ts4 = ts3 + 1000;

        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(entry.getKey());
        hTable.init();
        Put put = new Put(toBytes(key));
        Delete delete = new Delete(toBytes(key));
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            put.addColumn(toBytes(family), toBytes(column + family), ts1, toBytes(value + ts1));
            put.addColumn(toBytes(family), toBytes(column + family), ts2, toBytes(value + ts2));
            put.addColumn(toBytes(family), toBytes(column + family), ts3, toBytes(value + ts3));
            put.addColumn(toBytes(family), toBytes(column + family), ts4, toBytes(value + ts4));
        }
        hTable.put(put);
        hTable.delete(delete);


        Get get = new Get(toBytes(key));
        get.setMaxVersions();
        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            get.addColumn(toBytes(family), toBytes(column + family));
        }
        Result result = hTable.get(get);
        Assert.assertEquals(0, result.size());
    }
    
    @Test
    public void testDelete() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartDeleteTest::testDeleteSpecifiedImpl);
    }
    @Test
    public void testDeleteLastVersion() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartDeleteTest::testDeleteLastVersionImpl);
    }
    
    @Test
    public void testDeleteColumn() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartDeleteTest::testDeleteColumnImpl);
    }
    
    @Test
    public void testDeleteFamily() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartDeleteTest::testDeleteFamilyImpl);
    }
    
    @Test
    public void testDeleteFamilyVersion() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartDeleteTest::testDeleteFamilyVersionImpl);
    }
    
    
    @Test
    public void testMultiCFDelete() throws Exception {
        FOR_EACH(group2tableNames, OHTableSecondaryPartDeleteTest::testMultiCFDeleteLastVersionImpl);
    }
    
    @Test 
    public void testMultiCFDeleteSpecified() throws Exception {
        FOR_EACH(group2tableNames, OHTableSecondaryPartDeleteTest::testMultiCFDeleteSpecifiedImpl);
    }
    
    @Test
    public void testMultiCFDeleteColumn() throws Exception {
        FOR_EACH(group2tableNames, OHTableSecondaryPartDeleteTest::testMultiCFDeleteColumnImpl);
    }
    
    @Test
    public void testMultiCFDeleteFamily() throws Exception {
        FOR_EACH(group2tableNames, OHTableSecondaryPartDeleteTest::testMultiCFDeleteFamilyImpl);
    }
    
    @Test
    public void testMultiCFDeleteFamilyVersion() throws Exception {
        FOR_EACH(group2tableNames, OHTableSecondaryPartDeleteTest::testMultiCFDeleteFamilyVersionImpl);
    }
    
    
    @Test
    public void testDeleteAll() throws Exception {
        FOR_EACH(group2tableNames, OHTableSecondaryPartDeleteTest::testDeleteAllImpl);
    }
}
