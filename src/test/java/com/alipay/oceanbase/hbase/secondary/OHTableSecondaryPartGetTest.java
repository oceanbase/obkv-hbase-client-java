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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;


public class OHTableSecondaryPartGetTest {
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


    public static void testGetImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column = "putColumn";
        String value = "value";
        Put put = new Put(toBytes(key));
        put.add(family.getBytes(), column.getBytes(), toBytes(value));
        hTable.put(put);

        Get get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        Result result = hTable.get(get);
        Cell[] cells = result.rawCells();
        assertEquals(1, cells.length);
        assertEquals(column, Bytes.toString(CellUtil.cloneQualifier(cells[0])));
        assertEquals("value", Bytes.toString(CellUtil.cloneValue(cells[0])));
        System.out.println("get table " + tableName + " done");

        hTable.close();
    }
    
    public static void testMultiCFGetImpl(Map.Entry<String, List<String>> entry) throws Exception {
        String key = "putKey";
        String column = "putColumn";

        for (String tableName : entry.getValue()) {
            String family = getColumnFamilyName(tableName);
            String value = family + "_value";
            long timestamp = System.currentTimeMillis();

            OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
            hTable.init();
            Put put = new Put(toBytes(key));
            put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
            hTable.put(put);

            Get get = new Get(key.getBytes());
            get.addFamily(family.getBytes());
            Result r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);

            hTable.close();
        }
    }
    
    
    @Test
    public void testGet() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartGetTest::testGetImpl);
    }
    
    @Test
    public void testMultiCFGet() throws Exception {
        FOR_EACH(group2tableNames, OHTableSecondaryPartGetTest::testMultiCFGetImpl);
    }
}
