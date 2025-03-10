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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static org.junit.Assert.assertEquals;

public class OHTableSecondaryPartAppendTest {
    private static List<String> tableNames = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = null;


    @BeforeClass
    public static void before() throws Exception {
        for (TableTemplateManager.TableType type : TableTemplateManager.TableType.values()) {
            createTables(type, tableNames, group2tableNames, true);
        }
    }

    @AfterClass
    public static void finish() throws Exception {
        dropTables(tableNames, group2tableNames);
    }

    @Before
    public void prepareCase() throws Exception {
        truncateTables(tableNames, group2tableNames);
    }
    
    public static void testAppendImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        String value = "value";
        Append append = new Append(key.getBytes());
        KeyValue kv1 = new KeyValue(key.getBytes(), family.getBytes(), column1.getBytes(), value.getBytes());
        KeyValue kv2 = new KeyValue(key.getBytes(), family.getBytes(), column2.getBytes(), value.getBytes());
        append.add(kv1);
        append.add(kv2);
        hTable.append(append);

        Get get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        Result result = hTable.get(get);
        Cell[] cells = result.rawCells();
        assertEquals(2, cells.length);

        for (Cell cell : cells) {
            if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(column1)) {
                assertEquals("valuevalue", Bytes.toString(CellUtil.cloneValue(cell)));
            } else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(column2)) {
                assertEquals("valuevalue", Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        hTable.close();
    }
    
    @Test
    public void testAppend() throws Exception {
        FOR_EACH(tableNames, OHTableSecondaryPartAppendTest::testAppendImpl);
    }
}
