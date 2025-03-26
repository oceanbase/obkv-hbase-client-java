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
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHTableSecondaryPartBatchGetTest {
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

    public static void testBatchGetImpl(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String family = getColumnFamilyName(tableName);
        String key = "putKey";
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        long timestamp = System.currentTimeMillis();
        Put put = new Put(toBytes(key));
        put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes("1"));
        put.add(family.getBytes(), column2.getBytes(), timestamp, toBytes("1"));
        hTable.put(put);

        List<Get> gets = new ArrayList<>();
        Get get1 = new Get(key.getBytes());
        get1.addFamily(family.getBytes());
        gets.add(get1);

        Get get2 = new Get(key.getBytes());
        get2.addColumn(family.getBytes(), column1.getBytes());
        gets.add(get2);

        Get get3 = new Get(key.getBytes());
        get3.addColumn(family.getBytes(), column2.getBytes());
        gets.add(get3);


        Result[] results = hTable.get(gets);
        for (Result result : results) {
            for (Cell cell : result.listCells()) {
                String Q = Bytes.toString(CellUtil.cloneQualifier(cell));
                String V = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("Column: " + Q + ", Value: " + V);
            }
        }
        hTable.close();
    }

    @Test
    public void testBatchGet() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartBatchGetTest::testBatchGetImpl);
    }
}
