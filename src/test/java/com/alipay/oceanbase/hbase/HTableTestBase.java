/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2022 OceanBase Group
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

package com.alipay.oceanbase.hbase;

import com.alipay.oceanbase.hbase.exception.FeatureNotSupportedException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ALL;
import static org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ONE;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.*;

public abstract class HTableTestBase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected Table          hTable;

    @Test
    public void testTableGroup() throws IOError, IOException {
        /*
        CREATE TABLEGROUP test SHARDING = 'ADAPTIVE';
        CREATE TABLE `test$family_group` (
                      `K` varbinary(1024) NOT NULL,
                      `Q` varbinary(256) NOT NULL,
                      `T` bigint(20) NOT NULL,
                      `V` varbinary(1024) DEFAULT NULL,
                      PRIMARY KEY (`K`, `Q`, `T`)
                ) TABLEGROUP = test;
         */
        String key = "putKey";
        String column1 = "putColumn1";
        String value = "value333444";
        long timestamp = System.currentTimeMillis();
        // put data
        Put put = new Put(toBytes(key));
        put.add("family_group".getBytes(), column1.getBytes(), timestamp, toBytes(value + "1"));
        hTable.put(put);
        // test get with empty family
        Get get = new Get(toBytes(key));
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);
        for (KeyValue keyValue : r.raw()) {
            Assert.assertEquals(key, Bytes.toString(keyValue.getRow()));
            Assert.assertEquals(column1, Bytes.toString(keyValue.getQualifier()));
            Assert.assertEquals(timestamp, keyValue.getTimestamp());
            Assert.assertEquals(value + "1", Bytes.toString(keyValue.getValue()));
            System.out.println("rowKey: " + new String(keyValue.getRow()) + " family :"
                               + new String(keyValue.getFamily()) + " columnQualifier:"
                               + new String(keyValue.getQualifier()) + " timestamp:"
                               + keyValue.getTimestamp() + " value:"
                               + new String(keyValue.getValue()));
        }

        get = new Get(toBytes(key));
        get.setTimeStamp(r.raw()[0].getTimestamp());
        get.setMaxVersions(1);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);
        for (KeyValue keyValue : r.raw()) {
            Assert.assertEquals(key, Bytes.toString(keyValue.getRow()));
            Assert.assertEquals(column1, Bytes.toString(keyValue.getQualifier()));
            Assert.assertEquals(timestamp, keyValue.getTimestamp());
            Assert.assertEquals(value + "1", Bytes.toString(keyValue.getValue()));
            System.out.println("rowKey: " + new String(keyValue.getRow()) + " family :"
                               + new String(keyValue.getFamily()) + " columnQualifier:"
                               + new String(keyValue.getQualifier()) + " timestamp:"
                               + keyValue.getTimestamp() + " value:"
                               + new String(keyValue.getValue()));
        }

        // test scan with empty family
        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                Assert.assertEquals(key, Bytes.toString(keyValue.getRow()));
                Assert.assertEquals(column1, Bytes.toString(keyValue.getQualifier()));
                Assert.assertEquals(timestamp, keyValue.getTimestamp());
                Assert.assertEquals(value + "1", Bytes.toString(keyValue.getValue()));
                System.out.println("rowKey: " + new String(keyValue.getRow()) + " family :"
                                   + new String(keyValue.getFamily()) + " columnQualifier:"
                                   + new String(keyValue.getQualifier()) + " timestamp:"
                                   + keyValue.getTimestamp() + " value:"
                                   + new String(keyValue.getValue()));
            }
        }
    }

    @Test
    public void testBasic() throws Exception {
        testBasic("family1");
    }

    private void testBasic(String family) throws Exception {
        String key = "putKey";
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        String value = "value";
        long timestamp = System.currentTimeMillis();
        Delete delete = new Delete(key.getBytes());
        delete.deleteFamily(family.getBytes());
        hTable.delete(delete);

        Put put = new Put(toBytes(key));
        put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(value));
        hTable.put(put);
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), toBytes(column1));
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        for (KeyValue keyValue : r.raw()) {
            System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                               + new String(keyValue.getQualifier()) + " timestamp:"
                               + keyValue.getTimestamp() + " value:"
                               + new String(keyValue.getValue()));
            Assert.assertEquals(key, Bytes.toString(keyValue.getRow()));
            Assert.assertEquals(column1, Bytes.toString(keyValue.getQualifier()));
            Assert.assertEquals(timestamp, keyValue.getTimestamp());
            Assert.assertEquals(value, Bytes.toString(keyValue.getValue()));
        }

        put = new Put(toBytes(key));
        put.add(family.getBytes(), column1.getBytes(), timestamp + 1, toBytes(value));
        hTable.put(put);
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), toBytes(column1));
        get.setMaxVersions(2);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        get.setMaxVersions(1);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        delete = new Delete(key.getBytes());
        delete.deleteFamily(family.getBytes());
        hTable.delete(delete);

        for (KeyValue keyValue : r.raw()) {
            System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                               + new String(keyValue.getQualifier()) + " timestamp:"
                               + keyValue.getTimestamp() + " value:"
                               + new String(keyValue.getValue()));
            Assert.assertEquals(key, Bytes.toString(keyValue.getRow()));
            Assert.assertEquals(column1, Bytes.toString(keyValue.getQualifier()));
            Assert.assertEquals(timestamp + 1, keyValue.getTimestamp());
            Assert.assertEquals(value, Bytes.toString(keyValue.getValue()));
        }

        try {
            for (int j = 0; j < 10; j++) {
                put = new Put((key + "_" + j).getBytes());
                put.add(family.getBytes(), column1.getBytes(), timestamp + 2, toBytes(value));
                put.add(family.getBytes(), column2.getBytes(), timestamp + 2, toBytes(value));
                hTable.put(put);
            }

            Scan scan = new Scan();
            scan.addColumn(family.getBytes(), column1.getBytes());
            scan.addColumn(family.getBytes(), column2.getBytes());
            scan.setStartRow(toBytes(key + "_" + 0));
            scan.setStopRow(toBytes(key + "_" + 9));
            scan.setMaxVersions(1);
            ResultScanner scanner = hTable.getScanner(scan);
            int i = 0;
            int count = 0;
            for (Result result : scanner) {
                boolean countAdd = true;
                for (KeyValue keyValue : result.raw()) {
                    System.out.println("rowKey: " + new String(keyValue.getRow())
                                       + " columnQualifier:" + new String(keyValue.getQualifier())
                                       + " timestamp:" + keyValue.getTimestamp() + " value:"
                                       + new String(keyValue.getValue()));
                    Assert.assertEquals(key + "_" + i, Bytes.toString(keyValue.getRow()));
                    Assert.assertTrue(column1.equals(Bytes.toString(keyValue.getQualifier()))
                                      || column2.equals(Bytes.toString(keyValue.getQualifier())));
                    Assert.assertEquals(timestamp + 2, keyValue.getTimestamp());
                    Assert.assertEquals(value, Bytes.toString(keyValue.getValue()));
                    if (countAdd) {
                        countAdd = false;
                        count++;
                    }
                }
                i++;
            }

            Assert.assertEquals(9, count);

            // scan.setBatch(1);

            scan.setMaxVersions(9);
            scanner = hTable.getScanner(scan);
            i = 0;
            count = 0;
            for (Result result : scanner) {
                boolean countAdd = true;
                for (KeyValue keyValue : result.raw()) {
                    System.out.println("rowKey: " + new String(keyValue.getRow())
                                       + " columnQualifier:" + new String(keyValue.getQualifier())
                                       + " timestamp:" + keyValue.getTimestamp() + " value:"
                                       + new String(keyValue.getValue()));
                    Assert.assertEquals(key + "_" + i, Bytes.toString(keyValue.getRow()));
                    Assert.assertTrue(column1.equals(Bytes.toString(keyValue.getQualifier()))
                                      || column2.equals(Bytes.toString(keyValue.getQualifier())));
                    Assert.assertEquals(value, Bytes.toString(keyValue.getValue()));
                    if (countAdd) {
                        countAdd = false;
                        count++;
                    }
                }
                i++;
            }

            Assert.assertEquals(9, count);
        } finally {
            for (int j = 0; j < 10; j++) {
                delete = new Delete(toBytes(key + "_" + j));
                delete.deleteFamily(family.getBytes());
                hTable.delete(delete);
            }
        }

    }

    @Test
    public void testHugeData() throws IOException {
        int testNum = 1000;
        String key = "putkey";
        byte[] keyBytes = key.getBytes();
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        String value = "value";
        String family = "family1";
        long startTimeBase = 1539700745718L;
        for (int j = 1; j <= testNum; j++) {
            byte[] rowkey = new byte[keyBytes.length + 8];
            long current = startTimeBase + j * 10;
            // System.out.println(current);
            byte[] currentBytes = Bytes.toBytes(current);
            System.arraycopy(keyBytes, 0, rowkey, 0, keyBytes.length);
            System.arraycopy(currentBytes, 0, rowkey, keyBytes.length, currentBytes.length);
            Put put = new Put(rowkey);
            put.add("family1".getBytes(), column1.getBytes(), toBytes(value));
            put.add("family1".getBytes(), column2.getBytes(), toBytes(value));
            hTable.put(put);
            if (0 == j % 250) {
                System.out.println("has put " + j + " rows");
            }
        }

        Scan scan = new Scan();
        scan.addColumn("family1".getBytes(), column1.getBytes());
        scan.addColumn("family1".getBytes(), column2.getBytes());
        byte[] start = new byte[keyBytes.length + 8];
        byte[] startTime = Bytes.toBytes(startTimeBase + 1000);
        byte[] end = new byte[keyBytes.length + 8];
        byte[] endTime = Bytes.toBytes(startTimeBase + 100000);
        System.arraycopy(keyBytes, 0, start, 0, keyBytes.length);
        System.arraycopy(startTime, 0, start, keyBytes.length, startTime.length);
        System.arraycopy(keyBytes, 0, end, 0, keyBytes.length);
        System.arraycopy(endTime, 0, end, keyBytes.length, endTime.length);
        scan.setStartRow(start);
        scan.setStopRow(end);
        scan.setMaxVersions(1);
        // scan.setBatch(100);

        ResultScanner scanner = hTable.getScanner(scan);
        int i = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                byte[] rowkey = keyValue.getRow();
                byte[] readKey = new byte[keyBytes.length];
                System.arraycopy(rowkey, 0, readKey, 0, keyBytes.length);
                byte[] timestamp = new byte[8];
                System.arraycopy(rowkey, keyBytes.length, timestamp, 0, 8);
                // System.out.println("key :" + Bytes.toString(readKey) + Bytes.toLong(timestamp)
                //                    + " column :" + Bytes.toString(keyValue.getQualifier()));
            }
            i++;
        }
        System.out.println("count " + i);
        assertEquals(testNum - 100 + 1, i);
    }

    @Test
    public void testMultiPut() throws IOException {
        String key1 = "getKey1";
        String key2 = "getKey2";
        String column1 = "column1";
        String column2 = "column2";
        String value1 = "value1";
        String family = "family1";
        Put put1 = new Put(Bytes.toBytes("testKey"));
        put1.add(toBytes(family), toBytes(column2), toBytes(value1));
        put1.add(toBytes(family), toBytes(column2), System.currentTimeMillis(), toBytes(value1));

        Put put2 = new Put(Bytes.toBytes("testKey1"));
        put2.add(toBytes(family), toBytes(column2), toBytes(value1));
        put2.add(toBytes(family), toBytes(column2), System.currentTimeMillis(), toBytes(value1));
        List<Put> puts = new ArrayList<>();
        puts.add(put1);
        puts.add(put2);
        hTable.put(puts);

        // put same k, q, t
        Put put3 = new Put(Bytes.toBytes("testKey"));
        put3.add(toBytes(family), toBytes(column1), 0L, toBytes(value1));
        put3.add(toBytes(family), toBytes(column1), 0L, toBytes(value1));

        Put put4 = new Put(Bytes.toBytes("testKey"));
        put4.add(toBytes(family), toBytes(column1), System.currentTimeMillis(), toBytes(value1));
        put4.add(toBytes(family), toBytes(column1), System.currentTimeMillis(), toBytes(value1));
        puts = new ArrayList<Put>();
        puts.add(put3);
        puts.add(put4);
        hTable.put(puts);
    }

    @Ignore
    public void testMultiPartitionPut() throws IOException {
        String[] keys = new String[] { "putKey1", "putKey2", "putKey3", "putKey4", "putKey5",
                "putKey6", "putKey7", "putKey8", "putKey9", "putKey10" };

        String column1 = "column1";
        String column2 = "column2";
        String column3 = "column3";
        String value = "value";
        String family = "familyPartition";
        // put
        {
            List<Put> puts = new ArrayList<Put>();
            for (String key : keys) {
                Put put = new Put(Bytes.toBytes(key));
                put.add(toBytes(family), toBytes(column1), toBytes(value));
                put.add(toBytes(family), toBytes(column2), System.currentTimeMillis(),
                    toBytes(value));
                puts.add(put);
            }

            for (String key : keys) {
                // put same k, q, t
                Put put = new Put(Bytes.toBytes(key));
                put.add(toBytes(family), toBytes(column3), 100L, toBytes(value));
                put.add(toBytes(family), toBytes(column3), 100L, toBytes(value));
                puts.add(put);
            }
            hTable.put(puts);
        }
        // get
        {
            List<Get> gets = new ArrayList<Get>();
            for (String key : keys) {
                Get get = new Get(Bytes.toBytes(key));
                get.addColumn(toBytes(family), toBytes(column1));
                get.addColumn(toBytes(family), toBytes(column2));
                get.addColumn(toBytes(family), toBytes(column3));
                gets.add(get);
            }
            Result[] res = hTable.get(gets);
            assertEquals(res.length, 10);
            assertEquals(res[0].raw().length, 3);
        }
    }

    @Ignore
    public void testMultiPartitionDel() throws IOException {
        String[] keys = new String[] { "putKey1", "putKey2", "putKey3", "putKey4", "putKey5",
                "putKey6", "putKey7", "putKey8", "putKey9", "putKey10" };

        String column1 = "column1";
        String column2 = "column2";
        String column3 = "column3";
        String value = "value";
        String family = "familyPartition";
        // delete
        {
            List<Delete> deletes = new ArrayList<Delete>();
            for (String key : keys) {
                Delete del = new Delete(Bytes.toBytes(key));
                del.deleteColumns(toBytes(family), toBytes(column1));
                del.deleteColumns(toBytes(family), toBytes(column2), System.currentTimeMillis());
                deletes.add(del);
            }

            for (String key : keys) {
                // del same k, q, t
                Delete del = new Delete(Bytes.toBytes(key));
                del.deleteColumn(toBytes(family), toBytes(column3), 100L);
                del.deleteColumn(toBytes(family), toBytes(column3), 100L);
                deletes.add(del);
            }
            hTable.delete(deletes);
        }
        // get
        {
            List<Get> gets = new ArrayList<Get>();
            for (String key : keys) {
                Get get = new Get(Bytes.toBytes(key));
                get.addColumn(toBytes(family), toBytes(column1));
                get.addColumn(toBytes(family), toBytes(column2));
                get.addColumn(toBytes(family), toBytes(column3));
                gets.add(get);
            }
            Result[] res = hTable.get(gets);
            assertEquals(res.length, 10);
            int i = 0;
            for (i = 0; i < res.length; ++i) {
                assertEquals(res[i].raw().length, 0);
            }
        }
    }

    public void tryPut(Table hTable, Put put) throws Exception {
        hTable.put(put);
        Thread.sleep(1);
    }

    @Test
    public void testFilter() throws Exception {
        String key1 = "getKey1";
        String key2 = "getKey2";
        String column1 = "abc";
        String column2 = "def";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";
        String family = "family1";
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.deleteFamily(toBytes(family));

        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.deleteFamily(toBytes(family));

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Get get;
        Result r;
        ColumnPrefixFilter filter;

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);

//        time may be different
//        +---------+-----+----------------+--------+
//        | K       | Q   | T              | V      |
//        +---------+-----+----------------+--------+
//        | getKey1 | abc | -1728834971469 | value1 |
//        | getKey1 | abc | -1728834971399 | value2 |
//        | getKey1 | abc | -1728834971330 | value1 |
//        | getKey1 | def | -1728834971748 | value2 |
//        | getKey1 | def | -1728834971679 | value1 |
//        | getKey1 | def | -1728834971609 | value2 |
//        | getKey1 | def | -1728834971540 | value1 |
//        | getKey2 | def | -1728834971887 | value2 |
//        | getKey2 | def | -1728834971818 | value1 |
//        +---------+-----+----------------+--------+

        SingleColumnValueFilter singleColumnValueFilter;
        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
                Bytes.toBytes(column1), CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value1)));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter;
        singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes(family),
                Bytes.toBytes(column1), CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value1)));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueExcludeFilter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);

        DependentColumnFilter dependentColumnFilter = new DependentColumnFilter(Bytes.toBytes(family),
                Bytes.toBytes(column1), false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(dependentColumnFilter);
        r = hTable.get(get);
        Assert.assertEquals(3, r.raw().length);

        dependentColumnFilter = new DependentColumnFilter(Bytes.toBytes(family),
                Bytes.toBytes(column1), true);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(dependentColumnFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        dependentColumnFilter = new DependentColumnFilter(Bytes.toBytes(family),
                Bytes.toBytes(column1), false, CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value2)));
        get = new Get(toBytes(key2));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(dependentColumnFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        dependentColumnFilter = new DependentColumnFilter(Bytes.toBytes(family),
                Bytes.toBytes(column2), false, CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value2)));
        get = new Get(toBytes(key2));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(dependentColumnFilter);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);



        filter = new ColumnPrefixFilter(Bytes.toBytes("e"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        filter = new ColumnPrefixFilter(Bytes.toBytes("a"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filter);
        r = hTable.get(get);
        Assert.assertEquals(3, r.raw().length);

        filter = new ColumnPrefixFilter(Bytes.toBytes("d"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);

        filter = new ColumnPrefixFilter(Bytes.toBytes("b"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        FilterList filterList = new FilterList(MUST_PASS_ONE);
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("a")));
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("d")));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        filterList = new FilterList(MUST_PASS_ALL);
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("a")));
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("d")));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        filterList = new FilterList(MUST_PASS_ONE);
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("c")));
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("d")));
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("e")));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);

        filterList = new FilterList(MUST_PASS_ALL);
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("d")));
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("de")));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);

        ColumnPaginationFilter f = new ColumnPaginationFilter(8, 0);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(f);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        Put putKey1Column3Value1 = new Put(toBytes(key1));
        putKey1Column3Value1.add(toBytes(family), toBytes("ggg"), toBytes(value1));
        tryPut(hTable, putKey1Column3Value1);

        f = new ColumnPaginationFilter(8, 0);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(f);
        r = hTable.get(get);
        Assert.assertEquals(3, r.raw().length);

        f = new ColumnPaginationFilter(8, Bytes.toBytes("abc"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(f);
        r = hTable.get(get);
        Assert.assertEquals(3, r.raw().length);

        f = new ColumnPaginationFilter(8, Bytes.toBytes("bc"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(f);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        f = new ColumnPaginationFilter(8, Bytes.toBytes("ef"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(f);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        f = new ColumnPaginationFilter(8, Bytes.toBytes("h"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(f);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        Scan scan;
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        ResultScanner scanner = hTable.getScanner(scan);

        int res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                if (res_count < 8) {
                    Assert.assertArrayEquals(key1.getBytes(), keyValue.getRow());
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), keyValue.getRow());
                }
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 10);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        filter = new ColumnPrefixFilter(Bytes.toBytes("d"));
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                if (res_count < 4) {
                    Assert.assertArrayEquals(key1.getBytes(), keyValue.getRow());
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), keyValue.getRow());
                }
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 6);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        filterList = new FilterList(MUST_PASS_ONE);
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("c")));
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("d")));
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("e")));
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("g")));
        scan.setFilter(filterList);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                if (res_count < 5) {
                    Assert.assertArrayEquals(key1.getBytes(), keyValue.getRow());
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), keyValue.getRow());
                }
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 7);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        f = new ColumnPaginationFilter(2, Bytes.toBytes("d"));
        scan.setFilter(f);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                if (res_count < 2) {
                    Assert.assertArrayEquals(key1.getBytes(), keyValue.getRow());
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), keyValue.getRow());
                }
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 3);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        f = new ColumnPaginationFilter(2, Bytes.toBytes("g"));
        scan.setFilter(f);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                if (res_count < 1) {
                    Assert.assertArrayEquals(key1.getBytes(), keyValue.getRow());
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), keyValue.getRow());
                }
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 1);
        scanner.close();

        Delete deleteKey3Family = new Delete(toBytes("getKey3"));
        deleteKey3Family.deleteFamily(toBytes(family));
        Delete deleteKey4Family = new Delete(toBytes("getKey4"));
        deleteKey4Family.deleteFamily(toBytes(family));
        hTable.delete(deleteKey3Family);
        hTable.delete(deleteKey4Family);

        Put putKey3Column3Value1 = new Put(toBytes("getKey3"));
        putKey3Column3Value1.add(toBytes(family), toBytes(column1), toBytes(value1));
        tryPut(hTable, putKey3Column3Value1);
        Put putKey4Column3Value1 = new Put(toBytes("getKey4"));
        putKey4Column3Value1.add(toBytes(family), toBytes(column1), toBytes(value1));
        tryPut(hTable, putKey4Column3Value1);

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey5".getBytes());
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 12);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey5".getBytes());
        scan.setMaxVersions(10);
        RandomRowFilter rf = new RandomRowFilter(-1);
        scan.setFilter(rf);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 0);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey5".getBytes());
        scan.setMaxVersions(10);
        rf = new RandomRowFilter(2);
        scan.setFilter(rf);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 12);
        scanner.close();

        long timestamp = System.currentTimeMillis();
        putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.add(toBytes(family), toBytes(column1), timestamp, toBytes(value1));

        putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.add(toBytes(family), toBytes(column1), timestamp, toBytes(value1));

        putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        // putKey1Column1Value1 and putKey2Column1Value1 have the same timestamp
        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column1Value1);
        tryPut(hTable, putKey2Column2Value2);

        dependentColumnFilter = new DependentColumnFilter(Bytes.toBytes(family),
                Bytes.toBytes(column1), false,  CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value1)));
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        scan.setFilter(dependentColumnFilter);
        scanner = hTable.getScanner(scan);

        long prevTimestamp = - 1;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                if (prevTimestamp == -1) {
                    prevTimestamp = keyValue.getTimestamp();
                } else {
                    Assert.assertEquals(prevTimestamp, keyValue.getTimestamp());
                }
            }
        }
        scanner.close();
    }

    @Test
    public void testFilter2() throws Exception {
        String key1 = "getKey1";
        String key2 = "getKey2";
        String column1 = "abc";
        String column2 = "def";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";
        String family = "family1";
        long   ts;
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.deleteFamily(toBytes(family));

        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.deleteFamily(toBytes(family));

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Get get;
        Result r;

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);

        FirstKeyOnlyFilter filter = new FirstKeyOnlyFilter();
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filter);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        KeyOnlyFilter kFilter = new KeyOnlyFilter();
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(kFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        ts = r.rawCells()[0].getTimestamp();

        List<Long> tss = new ArrayList<>();
        tss.add(ts - 1);
        tss.add(ts);
        tss.add(ts + 1);
        TimestampsFilter tFilter = new TimestampsFilter(tss);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(tFilter);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        tss = new ArrayList<>();
        tss.add(ts - 1);
        tss.add(ts + 1);
        tFilter = new TimestampsFilter(tss);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(tFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        Put putKey1Column3Value1 = new Put(toBytes(key1));
        putKey1Column3Value1.add(toBytes(family), toBytes("ggg"), toBytes(value1));
        tryPut(hTable, putKey1Column3Value1);

        Scan scan;
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        ResultScanner scanner = hTable.getScanner(scan);

        int res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                if (res_count < 8) {
                    Assert.assertArrayEquals(key1.getBytes(), keyValue.getRow());
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), keyValue.getRow());
                }
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 10);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        filter = new FirstKeyOnlyFilter();
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                if (res_count < 1) {
                    Assert.assertArrayEquals(key1.getBytes(), keyValue.getRow());
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), keyValue.getRow());
                }
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 2);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        kFilter = new KeyOnlyFilter();
        scan.setFilter(kFilter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                if (res_count < 8) {
                    Assert.assertArrayEquals(key1.getBytes(), keyValue.getRow());
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), keyValue.getRow());
                }
                Assert.assertEquals(0, keyValue.getValueLength());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 10);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        kFilter = new KeyOnlyFilter(true);
        scan.setFilter(kFilter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                if (res_count < 8) {
                    Assert.assertArrayEquals(key1.getBytes(), cell.getRow());
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), cell.getRow());
                }
                Assert.assertEquals(4, cell.getValueLength());
                Assert.assertEquals(6, Bytes.toInt(CellUtil.cloneValue(cell)));
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 10);
        scanner.close();

        FilterList filterList = new FilterList(MUST_PASS_ONE);
        filterList.addFilter(new KeyOnlyFilter(false));
        filterList.addFilter(new KeyOnlyFilter(true));
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        scan.setFilter(filterList);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                if (res_count < 8) {
                    Assert.assertArrayEquals(key1.getBytes(), cell.getRow());
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), cell.getRow());
                }
                Assert.assertEquals(4, cell.getValueLength());
                Assert.assertEquals(0, Bytes.toInt(CellUtil.cloneValue(cell)));
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 10);
        scanner.close();

        filterList = new FilterList(MUST_PASS_ALL);
        filterList.addFilter(new KeyOnlyFilter(false));
        filterList.addFilter(new KeyOnlyFilter(true));
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        scan.setFilter(filterList);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                if (res_count < 8) {
                    Assert.assertArrayEquals(key1.getBytes(), cell.getRow());
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), cell.getRow());
                }
                Assert.assertEquals(4, cell.getValueLength());
                Assert.assertEquals(0, Bytes.toInt(CellUtil.cloneValue(cell)));
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 10);
        scanner.close();

        filterList = new FilterList(MUST_PASS_ONE);
        filterList.addFilter(new KeyOnlyFilter(true));
        filterList.addFilter(new KeyOnlyFilter(false));
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        scan.setFilter(filterList);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                if (res_count < 8) {
                    Assert.assertArrayEquals(key1.getBytes(), cell.getRow());
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), cell.getRow());
                }
                Assert.assertEquals(0, cell.getValueLength());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 10);
        scanner.close();

        filterList = new FilterList(MUST_PASS_ALL);
        filterList.addFilter(new KeyOnlyFilter(true));
        filterList.addFilter(new KeyOnlyFilter(false));
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        scan.setFilter(filterList);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                if (res_count < 8) {
                    Assert.assertArrayEquals(key1.getBytes(), cell.getRow());
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), cell.getRow());
                }
                Assert.assertEquals(0, cell.getValueLength());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 10);
        scanner.close();
    }

    @Test
    public void testGetFilter() throws Exception {
        String key1 = "getKey1";
        String key2 = "getKey2";
        String column1 = "column1";
        String column2 = "column2";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";
        String family = "family1";
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.deleteFamily(toBytes(family));

        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.deleteFamily(toBytes(family));

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Get get;
        Result r;

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);

        // prefix filter
        get = new Get(toBytes(key1));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        // prefix filter excluded
        get = new Get(toBytes(key1));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        PrefixFilter prefixFilter = new PrefixFilter(toBytes("aa"));
        get.setFilter(prefixFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        // prefix filter included
        get = new Get(toBytes(key1));
        get.addFamily(toBytes(family));
        prefixFilter = new PrefixFilter(toBytes("get"));
        get.setFilter(prefixFilter);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        get = new Get(toBytes(key1));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        prefixFilter = new PrefixFilter(toBytes("get"));
        get.setFilter(prefixFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);

        // columnCountGetFilter filter
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        // columnCountGetFilter filter 1
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        ColumnCountGetFilter columnCountGetFilter = new ColumnCountGetFilter(1);
        get.setFilter(columnCountGetFilter);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        DependentColumnFilter dependentColumnFilter = new DependentColumnFilter(Bytes.toBytes(family),
                Bytes.toBytes(column1));
        get.setFilter(dependentColumnFilter);
        r = hTable.get(get);
        Assert.assertEquals(3, r.raw().length);

        // columnCountGetFilter filter 2
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        columnCountGetFilter = new ColumnCountGetFilter(2);
        get.setFilter(columnCountGetFilter);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        //value filter

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(toBytes(value2)));
        get.setFilter(valueFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            toBytes(value1)));
        get.setFilter(valueFilter);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        hTable.put(putKey1Column1Value2);
        hTable.put(putKey1Column2Value2);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            toBytes(value2)));
        get.setFilter(valueFilter);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        valueFilter = new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(
            toBytes(value2)));
        get.setFilter(valueFilter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(
            toBytes(value1)));
        get.setFilter(valueFilter);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
            new BinaryComparator(toBytes(value1)));
        get.setFilter(valueFilter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(
            toBytes(value3)));
        get.setFilter(valueFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        //Qualifier Filter
        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(toBytes(column1)));
        get.setFilter(qualifierFilter);
        r = hTable.get(get);
        Assert.assertEquals(3, r.raw().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            toBytes(column2)));
        get.setFilter(qualifierFilter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.GREATER,
            new BinaryComparator(toBytes(column1)));
        get.setFilter(qualifierFilter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
            new BinaryComparator(toBytes(column1)));
        get.setFilter(qualifierFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        // filter list
        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);

        // show table (time maybe different)
        //+---------+---------+----------------+--------+
        //| K         | Q       | T              | V      |
        //+---------+---------+----------------+--------+
        //| getKey1 | column1 | -1709714109660 | value1 |
        //| getKey1 | column1 | -1709714109661 | value2 |
        //| getKey1 | column1 | -1709714109662 | value1 |
        //| getKey1 | column2 | -1709714109663 | value1 |
        //| getKey1 | column2 | -1709714109664 | value2 |
        //| getKey1 | column2 | -1709714109665 | value1 |
        //| getKey1 | column2 | -1709714109666 | value2 |
        //| getKey2 | column2 | -1709714109667 | value1 |
        //| getKey2 | column2 | -1709714109668 | value2 |
        //+---------+---------+----------------+--------+

        FilterList filterList = new FilterList();
        filterList.addFilter(new ColumnCountGetFilter(1));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        filterList = new FilterList();
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(family), Bytes
            .toBytes(column1), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value1)));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        filterList = new FilterList();
        filterList.addFilter(new SingleColumnValueExcludeFilter(Bytes.toBytes(family), Bytes
                .toBytes(column1), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value1)));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);

        filterList = new FilterList();
        filterList.addFilter(new DependentColumnFilter(Bytes.toBytes(family), Bytes
                .toBytes(column1), false));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(3, r.raw().length);

        filterList = new FilterList();
        filterList.addFilter(new DependentColumnFilter(Bytes.toBytes(family), Bytes
                .toBytes(column2), false));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);

        filterList = new FilterList();
        filterList.addFilter(new DependentColumnFilter(Bytes.toBytes(family), Bytes
                .toBytes(column2)));
        get = new Get(toBytes(key2));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        filterList = new FilterList();
        filterList.addFilter(new DependentColumnFilter(Bytes.toBytes(family), Bytes
                .toBytes(column2), true));
        get = new Get(toBytes(key2));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        filterList = new FilterList();
        filterList.addFilter(new DependentColumnFilter(Bytes.toBytes(family), Bytes
                .toBytes(column2), false, CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(toBytes(value2))));
        get = new Get(toBytes(key2));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        filterList = new FilterList();
        filterList.addFilter(new ColumnCountGetFilter(1));
        filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.GREATER,
            new BinaryComparator(toBytes(column2))));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        filterList = new FilterList(MUST_PASS_ONE);
        filterList.addFilter(new ColumnCountGetFilter(2));
        filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(toBytes(column2))));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(6, r.raw().length);

        filterList = new FilterList();
        filterList.addFilter(new ColumnCountGetFilter(2));
        filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(toBytes(column2))));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        filterList = new FilterList();
        filterList.addFilter(new ColumnCountGetFilter(2));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);
        Assert.assertFalse(Bytes.equals(r.raw()[0].getQualifier(), r.raw()[1].getQualifier()));

        filterList = new FilterList();
        filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(toBytes(column2))));
        filterList.addFilter(new ColumnCountGetFilter(2));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        filterList = new FilterList();
        filterList.addFilter(new ColumnCountGetFilter(2));
        filterList.addFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            toBytes(value2))));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        // test empty filter in FilterList
        filterList = new FilterList();
        FilterList emptyFilterList = new FilterList();
        filterList.addFilter(emptyFilterList);
        filterList.addFilter(new PageFilter(1));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        // test empty filter in FilterList
        filterList = new FilterList();
        emptyFilterList = new FilterList();
        filterList.addFilter(emptyFilterList);
        filterList.addFilter(new ColumnPaginationFilter(3, 1));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        // test empty filter in FilterList
        filterList = new FilterList();
        emptyFilterList = new FilterList();
        filterList.addFilter(emptyFilterList);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        // singleColumnValue Filter
        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);

        // 任何一个版本满足则返回本行
        SingleColumnValueFilter singleColumnValueFilter;
        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column1), CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value1)));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);


        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter;
        singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes(family),
                Bytes.toBytes(column1), CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value1)));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueExcludeFilter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);


        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column1), CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value2)));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column1), CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value2)));
        singleColumnValueFilter.setLatestVersionOnly(false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column2), CompareFilter.CompareOp.LESS, new BinaryComparator(
                toBytes(value1)));
        singleColumnValueFilter.setLatestVersionOnly(false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column2), CompareFilter.CompareOp.LESS, new BinaryComparator(
                toBytes(value2)));
        singleColumnValueFilter.setLatestVersionOnly(false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column2), CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(
                toBytes(value2)));
        singleColumnValueFilter.setLatestVersionOnly(false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column2), CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(
                toBytes(value2)));
        singleColumnValueFilter.setLatestVersionOnly(false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column2), CompareFilter.CompareOp.GREATER, new BinaryComparator(
                toBytes(value2)));
        singleColumnValueFilter.setLatestVersionOnly(false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        // Skip Filter
        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column2Value2);

        valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            toBytes(value2)));
        SkipFilter skipFilter = new SkipFilter(valueFilter);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(skipFilter);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        valueFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(
            toBytes(value2)));
        skipFilter = new SkipFilter(valueFilter);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(skipFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);

        valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            toBytes(value2)));
        skipFilter = new SkipFilter(valueFilter);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(skipFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        valueFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(
            toBytes(value2)));
        skipFilter = new SkipFilter(valueFilter);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(skipFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        // WhileMatchFilter
        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);

        WhileMatchFilter whileMatchFilter;

        valueFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(
            toBytes(value2)));
        whileMatchFilter = new WhileMatchFilter(valueFilter);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(whileMatchFilter);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);
    }

    @Test
    // todo: heyu
    public void testScanWithObParams() throws Exception {
        String key1 = "scanKey1x";
        String key2 = "scanKey2x";
        String key3 = "scanKey3x";
        String key4 = "scanKey4x";
        String column1 = "column1";
        String column2 = "column2";
        String value1 = "value1";
        String value2 = "value2";
        String family = "family1";

        // delete previous data
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.deleteFamily(toBytes(family));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.deleteFamily(toBytes(family));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.deleteFamily(toBytes(family));
        Delete deleteKey4Family = new Delete(toBytes(key4));
        deleteKey4Family.deleteFamily(toBytes(family));

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
        hTable.delete(deleteKey4Family);

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey4Column1Value1 = new Put(toBytes(key4));
        putKey4Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1); // 2 * putKey1Column1Value1
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1); // 2 * putKey1Column2Value1
        tryPut(hTable, putKey1Column2Value2); // 2 * putKey1Column2Value2
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);
        tryPut(hTable, putKey3Column1Value1);
        tryPut(hTable, putKey4Column1Value1);

        Scan scan;

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey5x".getBytes());
        scan.setMaxVersions(10);
        scan.setCaching(1);
        scan.setBatch(3);
        ResultScanner scanner = hTable.getScanner(scan);
        Result result = scanner.next();
        Assert.assertEquals(3, result.size());
        scanner.close();

        scan.setMaxResultSize(10);
        scan.setBatch(-1);
        ResultScanner scanner1 = hTable.getScanner(scan);
        result = scanner1.next();
        Assert.assertEquals(7, result.size()); // 返回第一行全部数据，因为不允许行内部分返回

        scanner1.close();

        scan.setAllowPartialResults(true);
        ResultScanner scanner2 = hTable.getScanner(scan);
        result = scanner2.next();
        Assert.assertEquals(1, result.size());

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
        hTable.delete(deleteKey4Family);
    }

    @Test
    public void testScanSessionClean() throws Exception {
        String key1 = "bKey";
        String key2 = "cKey";
        String key3 = "dKey";
        String key4 = "eKey";
        String key5 = "fKey";
        String column1 = "column1";
        String column2 = "column2";
        String value1 = "value1";
        String family = "family1";

        // delete previous data
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.deleteFamily(toBytes(family));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.deleteFamily(toBytes(family));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.deleteFamily(toBytes(family));
        Delete deleteKey4Family = new Delete(toBytes(key4));
        deleteKey4Family.deleteFamily(toBytes(family));
        Delete deleteKey5Family = new Delete(toBytes(key5));
        deleteKey5Family.deleteFamily(toBytes(family));

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
        hTable.delete(deleteKey4Family);
        hTable.delete(deleteKey5Family);

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey4Column2Value1 = new Put(toBytes(key4));
        putKey4Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey5Column2Value1 = new Put(toBytes(key5));
        putKey5Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey3Column2Value1);
        tryPut(hTable, putKey4Column2Value1);
        tryPut(hTable, putKey5Column2Value1);

        Scan scan;
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setBatch(1);

        ResultScanner scanner = hTable.getScanner(scan);
        scanner.next();

        // The server defaults to a lease of 60 seconds. Therefore, at 20 seconds,
        // the transaction is checked to ensure it has not rolled back, and the lease is updated.
        // At 55 seconds, the query should still be able to retrieve the data and update the lease.
        // If it exceeds 60 seconds (at 61 seconds), the session is deleted.
        Thread.sleep(20 * 1000);
        scanner.next();

        Thread.sleep(55 * 1000);
        scanner.next();

        Thread.sleep(61 * 1000);
        try {
            scanner.next();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause().getMessage().contains("OB_HASH_NOT_EXIST"));
        } finally {
            scanner.close();
        }
    }

    @Test
    public void testGet() throws Exception {
        String key1 = "scanKey1x";
        String key2 = "scanKey2x";
        String key3 = "scanKey3x";
        String zKey1 = "zScanKey1";
        String zKey2 = "zScanKey2";
        String column1 = "column1";
        String column2 = "column2";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";
        String family = "family1";

        // delete previous data
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.deleteFamily(toBytes(family));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.deleteFamily(toBytes(family));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.deleteFamily(toBytes(family));
        Delete deleteZKey1Family = new Delete(toBytes(zKey1));
        deleteZKey1Family.deleteFamily(toBytes(family));
        Delete deleteZKey2Family = new Delete(toBytes(zKey2));
        deleteZKey2Family.deleteFamily(toBytes(family));

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
        hTable.delete(deleteZKey1Family);
        hTable.delete(deleteZKey2Family);

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey3Column1Value2 = new Put(toBytes(key3));
        putKey3Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column2Value2 = new Put(toBytes(key3));
        putKey3Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putzKey1Column1Value1 = new Put(toBytes(zKey1));
        putzKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putzKey2Column1Value1 = new Put(toBytes(zKey2));
        putzKey2Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Get get;
        Result r;
        int res_count = 0;

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1); // 2 * putKey1Column1Value1
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1); // 2 * putKey1Column2Value1
        tryPut(hTable, putKey1Column2Value2); // 2 * putKey1Column2Value2
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);
        tryPut(hTable, putKey3Column1Value1);
        tryPut(hTable, putKey3Column1Value2);
        tryPut(hTable, putKey3Column2Value1);
        tryPut(hTable, putKey3Column2Value2);
        tryPut(hTable, putzKey1Column1Value1);
        tryPut(hTable, putzKey2Column1Value1);

        // show table (time maybe different)
        //+-----------+---------+----------------+--------+
        //| K         | Q       | T              | V      |
        //+-----------+---------+----------------+--------+
        //| scanKey1x | column1 | -1709714409669 | value1 |
        //| scanKey1x | column1 | -1709714409637 | value2 |
        //| scanKey1x | column1 | -1709714409603 | value1 |
        //| scanKey1x | column2 | -1709714409802 | value2 |
        //| scanKey1x | column2 | -1709714409768 | value1 |
        //| scanKey1x | column2 | -1709714409735 | value2 |
        //| scanKey1x | column2 | -1709714409702 | value1 |
        //| scanKey2x | column2 | -1709714409869 | value2 |
        //| scanKey2x | column2 | -1709714409836 | value1 |
        //| scanKey3x | column1 | -1709714409940 | value2 |
        //| scanKey3x | column1 | -1709714409904 | value1 |
        //| scanKey3x | column2 | -1709714410010 | value2 |
        //| scanKey3x | column2 | -1709714409977 | value1 |
        //+-----------+---------+----------------+--------+

        // test closestRowBefore
        get = new Get("scanKey2x2".getBytes());
        get.addFamily(family.getBytes());
        get.setClosestRowBefore(true);
        r = hTable.get(get);
        assertEquals(key2, Bytes.toString(r.getRow()));

        // test exists
        LinkedList<Get> gets = new LinkedList<>();
        Get get1 = new Get(key1.getBytes());
        get1.addFamily(family.getBytes());
        Get get2 = new Get(key3.getBytes());
        get2.addFamily(family.getBytes());
        Get get3 = new Get(key1.getBytes());
        get3.addFamily(family.getBytes());
        Get get4 = new Get("scanKey2x2".getBytes());
        get4.addFamily(family.getBytes());
        Get get5 = new Get(key2.getBytes());
        get5.addFamily(family.getBytes());
        gets.add(get1);
        gets.add(get2);
        gets.add(get3);
        gets.add(get4);
        gets.add(get5);
        boolean[] booleans = hTable.existsAll(gets);
        assertTrue(booleans[0]);
        assertTrue(booleans[1]);
        assertTrue(booleans[2]);
        assertFalse(booleans[3]);
        assertTrue(booleans[4]);
    }

    @Test
    public void testScan() throws Exception {
        String key1 = "scanKey1x";
        String key2 = "scanKey2x";
        String key3 = "scanKey3x";
        String zKey1 = "zScanKey1";
        String zKey2 = "zScanKey2";
        String column1 = "column1";
        String column2 = "column2";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";
        String family = "family1";

        // delete previous data
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.deleteFamily(toBytes(family));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.deleteFamily(toBytes(family));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.deleteFamily(toBytes(family));
        Delete deleteZKey1Family = new Delete(toBytes(zKey1));
        deleteZKey1Family.deleteFamily(toBytes(family));
        Delete deleteZKey2Family = new Delete(toBytes(zKey2));
        deleteZKey2Family.deleteFamily(toBytes(family));

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
        hTable.delete(deleteZKey1Family);
        hTable.delete(deleteZKey2Family);

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey3Column1Value2 = new Put(toBytes(key3));
        putKey3Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column2Value2 = new Put(toBytes(key3));
        putKey3Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putzKey1Column1Value1 = new Put(toBytes(zKey1));
        putzKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putzKey2Column1Value1 = new Put(toBytes(zKey2));
        putzKey2Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Get get;
        Scan scan;
        Result r;
        ResultScanner scanner;
        int res_count = 0;

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1); // 2 * putKey1Column1Value1
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1); // 2 * putKey1Column2Value1
        tryPut(hTable, putKey1Column2Value2); // 2 * putKey1Column2Value2
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);
        tryPut(hTable, putKey3Column1Value1);
        tryPut(hTable, putKey3Column1Value2);
        tryPut(hTable, putKey3Column2Value1);
        tryPut(hTable, putKey3Column2Value2);
        tryPut(hTable, putzKey1Column1Value1);
        tryPut(hTable, putzKey2Column1Value1);

        // show table (time maybe different)
        //+-----------+---------+----------------+--------+
        //| K         | Q       | T              | V      |
        //+-----------+---------+----------------+--------+
        //| scanKey1x | column1 | -1709714409669 | value1 |
        //| scanKey1x | column1 | -1709714409637 | value2 |
        //| scanKey1x | column1 | -1709714409603 | value1 |
        //| scanKey1x | column2 | -1709714409802 | value2 |
        //| scanKey1x | column2 | -1709714409768 | value1 |
        //| scanKey1x | column2 | -1709714409735 | value2 |
        //| scanKey1x | column2 | -1709714409702 | value1 |
        //| scanKey2x | column2 | -1709714409869 | value2 |
        //| scanKey2x | column2 | -1709714409836 | value1 |
        //| scanKey3x | column1 | -1709714409940 | value2 |
        //| scanKey3x | column1 | -1709714409904 | value1 |
        //| scanKey3x | column2 | -1709714410010 | value2 |
        //| scanKey3x | column2 | -1709714409977 | value1 |
        //+-----------+---------+----------------+--------+

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey3x".getBytes());
        scan.setMaxVersions(10);
        scan.setMaxResultsPerColumnFamily(2);
        scan.setRowOffsetPerColumnFamily(1);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            res_count += result.size();
        }
        Assert.assertEquals(3, res_count);
        scanner.close();

        // check insert ok
        get = new Get(toBytes(key1));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);
        get = new Get(toBytes(key2));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);
        get = new Get(toBytes(key3));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey2x".getBytes());
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                res_count += 1;
            }
        }
        Assert.assertEquals(7, res_count);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey3x".getBytes());
        scan.setMaxVersions(10);
        scan.setMaxResultsPerColumnFamily(1);
        scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            assertEquals(1, result.rawCells().length);
        }
        scanner.close();

        // test maxResultSize and AllowPartialResults
        scan = new Scan();
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey3x".getBytes());
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setMaxResultSize(100);
        scan.setAllowPartialResults(true);
        scanner = hTable.getScanner(scan);
        Result next = scanner.next();
        assertEquals(2, next.size());

        scanner.close();

        // test cacheSize
        scan = new Scan();
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey3x".getBytes());
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setCaching(1);
        scanner = hTable.getScanner(scan);
        next = scanner.next();
        assertEquals(7, next.size());

        scanner.close();

        // test maxResultSize and not AllowPartialResults
        scan = new Scan();
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey3x".getBytes());
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setMaxResultSize(100);
        scanner = hTable.getScanner(scan);
        next = scanner.next();
        assertEquals(7, next.size());

        scanner.close();

        // scan with prefixFilter
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey3x".getBytes());
        PrefixFilter prefixFilter = new PrefixFilter(toBytes("scanKey2"));
        scan.setFilter(prefixFilter);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                Arrays.equals(key2.getBytes(), keyValue.getRow());
                res_count += 1;
            }
        }
        Assert.assertEquals(2, res_count);
        scanner.close();

        // scan with singleColumnValueFilter
        // 任何一个版本满足则返回本行
        SingleColumnValueFilter singleColumnValueFilter;
        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column1), CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value1)));
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey3x".getBytes());
        scan.setFilter(singleColumnValueFilter);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                res_count += 1;
            }
        }
        Assert.assertEquals(9, res_count);
        scanner.close();

        // scan with HConstants.EMPTY_START_ROW / HConstants.EMPTY_END_ROW / HConstants.EMPTY_BYTE_ARRAY
        scan = new Scan("zScanKey".getBytes(), HConstants.EMPTY_END_ROW);
        scan.addFamily(family.getBytes());
        scan.setFilter(singleColumnValueFilter);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                res_count += 1;
            }
        }
        Assert.assertEquals(2, res_count);
        scanner.close();

        // try to delete all with scan
        scan = new Scan(HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY);
        scan.addFamily(family.getBytes());
        scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            Delete delete = new Delete(result.getRow());
            delete.deleteFamily(toBytes(family));
            hTable.delete(delete);
        }

        // verify table is empty
        scan = new Scan("scanKey".getBytes(), HConstants.EMPTY_BYTE_ARRAY);
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                res_count += 1;
            }
        }
        Assert.assertEquals(0, res_count);
        scanner.close();

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
        hTable.delete(deleteZKey1Family);
        hTable.delete(deleteZKey2Family);
    }

    @Test
    public void testReversedScan() throws Exception {
        String key1 = "scanKey1x";
        String key2 = "scanKey2x";
        String key3 = "scanKey3x";
        String zKey1 = "zScanKey1";
        String zKey2 = "zScanKey2";
        String column1 = "column1";
        String column2 = "column2";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";
        String family = "family1";

        // delete previous data
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.deleteFamily(toBytes(family));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.deleteFamily(toBytes(family));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.deleteFamily(toBytes(family));
        Delete deleteZKey1Family = new Delete(toBytes(zKey1));
        deleteZKey1Family.deleteFamily(toBytes(family));
        Delete deleteZKey2Family = new Delete(toBytes(zKey2));
        deleteZKey2Family.deleteFamily(toBytes(family));

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
        hTable.delete(deleteZKey1Family);
        hTable.delete(deleteZKey2Family);

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey3Column1Value2 = new Put(toBytes(key3));
        putKey3Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column2Value2 = new Put(toBytes(key3));
        putKey3Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Get get;
        Scan scan;
        Result r;
        int res_count = 0;

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1); // 2 * putKey1Column1Value1
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1); // 2 * putKey1Column2Value1
        tryPut(hTable, putKey1Column2Value2); // 2 * putKey1Column2Value2
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);
        tryPut(hTable, putKey3Column1Value1);
        tryPut(hTable, putKey3Column1Value2);
        tryPut(hTable, putKey3Column2Value1);
        tryPut(hTable, putKey3Column2Value2);

        // show table (time maybe different)
        // +-----------+---------+----------------+--------+
        // | K | Q | T | V |
        // +-----------+---------+----------------+--------+
        // | scanKey1x | column1 | -1709714409669 | value1 |
        // | scanKey1x | column1 | -1709714409637 | value2 |
        // | scanKey1x | column1 | -1709714409603 | value1 |
        // | scanKey1x | column2 | -1709714409802 | value2 |
        // | scanKey1x | column2 | -1709714409768 | value1 |
        // | scanKey1x | column2 | -1709714409735 | value2 |
        // | scanKey1x | column2 | -1709714409702 | value1 |
        // | scanKey2x | column2 | -1709714409869 | value2 |
        // | scanKey2x | column2 | -1709714409836 | value1 |
        // | scanKey3x | column1 | -1709714409940 | value2 |
        // | scanKey3x | column1 | -1709714409904 | value1 |
        // | scanKey3x | column2 | -1709714410010 | value2 |
        // | scanKey3x | column2 | -1709714409977 | value1 |
        // +-----------+---------+----------------+--------+

        // reverse scan
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey3x".getBytes());
        scan.setStopRow("scanKey1x".getBytes());
        scan.setReversed(true);
        scan.setMaxVersions(10);
        ResultScanner scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                Arrays.equals(key1.getBytes(), keyValue.getRow());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 6);
        scanner.close();

        // reverse scan with MaxVersion
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey3x".getBytes());
        scan.setStopRow("scanKey1x".getBytes());
        scan.setReversed(true);
        scan.setMaxVersions(1);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                Arrays.equals(key1.getBytes(), keyValue.getRow());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 3);
        scanner.close();

        // reverse scan with pageFilter
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey3x".getBytes());
        scan.setStopRow("scanKey1x".getBytes());
        PageFilter pageFilter = new PageFilter(2);
        scan.setFilter(pageFilter);
        scan.setReversed(true);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                Arrays.equals(key1.getBytes(), keyValue.getRow());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 6);
        scanner.close();

        // reverse scan with not_exist_start_row
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey4x".getBytes());
        scan.setStopRow("scanKey1x".getBytes());
        scan.setReversed(true);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                Arrays.equals(key1.getBytes(), keyValue.getRow());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 6);
        scanner.close();

        // reverse scan with abnormal range
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey3x".getBytes());
        scan.setReversed(true);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                Arrays.equals(key1.getBytes(), keyValue.getRow());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 0);
        scanner.close();

        // reverse scan with abnormal range
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey3x".getBytes());
        scan.setStopRow("scanKey0x".getBytes());
        scan.setReversed(true);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                Arrays.equals(key1.getBytes(), keyValue.getRow());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 13);
        scanner.close();

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);

    }

    @Test
    public void testPartitionScan() throws Exception {
        String key1 = "scanKey1x";
        String key2 = "scanKey2x";
        String key3 = "scanKey3x";
        String zKey1 = "zScanKey1";
        String zKey2 = "zScanKey2";
        String column1 = "column1";
        String column2 = "column2";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";
        String family = "partitionFamily1";

        // delete previous data
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.deleteFamily(toBytes(family));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.deleteFamily(toBytes(family));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.deleteFamily(toBytes(family));
        Delete deleteZKey1Family = new Delete(toBytes(zKey1));
        deleteZKey1Family.deleteFamily(toBytes(family));
        Delete deleteZKey2Family = new Delete(toBytes(zKey2));
        deleteZKey2Family.deleteFamily(toBytes(family));

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
        hTable.delete(deleteZKey1Family);
        hTable.delete(deleteZKey2Family);

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey3Column1Value2 = new Put(toBytes(key3));
        putKey3Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column2Value2 = new Put(toBytes(key3));
        putKey3Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putzKey1Column1Value1 = new Put(toBytes(zKey1));
        putzKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putzKey2Column1Value1 = new Put(toBytes(zKey2));
        putzKey2Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Get get;
        Scan scan;
        Result r;
        int res_count = 0;

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1); // 2 * putKey1Column1Value1
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1); // 2 * putKey1Column2Value1
        tryPut(hTable, putKey1Column2Value2); // 2 * putKey1Column2Value2
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);
        tryPut(hTable, putKey3Column1Value1);
        tryPut(hTable, putKey3Column1Value2);
        tryPut(hTable, putKey3Column2Value1);
        tryPut(hTable, putKey3Column2Value2);
        tryPut(hTable, putzKey1Column1Value1);
        tryPut(hTable, putzKey2Column1Value1);

        // show table (time maybe different)
        //+-----------+---------+----------------+--------+
        //| K         | Q       | T              | V      |
        //+-----------+---------+----------------+--------+
        //| scanKey1x | column1 | -1709714409669 | value1 |
        //| scanKey1x | column1 | -1709714409637 | value2 |
        //| scanKey1x | column1 | -1709714409603 | value1 |
        //| scanKey1x | column2 | -1709714409802 | value2 |
        //| scanKey1x | column2 | -1709714409768 | value1 |
        //| scanKey1x | column2 | -1709714409735 | value2 |
        //| scanKey1x | column2 | -1709714409702 | value1 |
        //| scanKey2x | column2 | -1709714409869 | value2 |
        //| scanKey2x | column2 | -1709714409836 | value1 |
        //| scanKey3x | column1 | -1709714409940 | value2 |
        //| scanKey3x | column1 | -1709714409904 | value1 |
        //| scanKey3x | column2 | -1709714410010 | value2 |
        //| scanKey3x | column2 | -1709714409977 | value1 |
        //+-----------+---------+----------------+--------+

        // check insert ok
        get = new Get(toBytes(key1));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(7, r.raw().length);

        get = new Get(toBytes(key2));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        get = new Get(toBytes(key3));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);

        // verify simple scan across partition
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey2x".getBytes());
        scan.setMaxVersions(10);
        ResultScanner scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                Arrays.equals(key1.getBytes(), keyValue.getRow());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 7);
        scanner.close();

        // scan with prefixFilter
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey3x".getBytes());
        PrefixFilter prefixFilter = new PrefixFilter(toBytes("scanKey2"));
        scan.setFilter(prefixFilter);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                Arrays.equals(key2.getBytes(), keyValue.getRow());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 2);
        scanner.close();

        // scan with singleColumnValueFilter
        // 任何一个版本满足则返回本行
        SingleColumnValueFilter singleColumnValueFilter;
        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column1), CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value1)));
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey3x".getBytes());
        scan.setFilter(singleColumnValueFilter);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                res_count += 1;
            }
        }
        Assert.assertEquals(9, res_count);
        scanner.close();

        // scan with HConstants.EMPTY_START_ROW / HConstants.EMPTY_END_ROW / HConstants.EMPTY_BYTE_ARRAY
        scan = new Scan("zScanKey".getBytes(), HConstants.EMPTY_END_ROW);
        scan.addFamily(family.getBytes());
        scan.setFilter(singleColumnValueFilter);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                res_count += 1;
            }
        }
        Assert.assertEquals(2, res_count);
        scanner.close();

        // try to delete all with scan
        scan = new Scan(HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY);
        scan.addFamily(family.getBytes());
        scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            Delete delete = new Delete(result.getRow());
            delete.deleteFamily(toBytes(family));
            hTable.delete(delete);
        }

        // verify table is empty
        scan = new Scan("scanKey".getBytes(), HConstants.EMPTY_BYTE_ARRAY);
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                res_count += 1;
            }
        }
        Assert.assertEquals(0, res_count);
        scanner.close();

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
        hTable.delete(deleteZKey1Family);
        hTable.delete(deleteZKey2Family);
    }

    @Ignore
    public void testDeleteIllegal() throws IOException {
        try {
            Delete delete = new Delete("key_5".getBytes());
            delete.deleteFamily("family2".getBytes());
            delete.deleteColumns("family1".getBytes(), "column1_1".getBytes(),
                System.currentTimeMillis());
            hTable.delete(delete);
            fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("multi family is not supported yet."));
        }

    }

    @Test
    public void testCheckAndMutationIllegal() throws IOException {
        // check and mute 只支持一行操作
        try {
            Put put = new Put("key_7".getBytes());
            put.add("family1".getBytes(), "column1_1".getBytes(), "value2".getBytes());
            boolean ret = hTable.checkAndPut("key_8".getBytes(), "family1".getBytes(),
                "column1_1".getBytes(), "value1".getBytes(), put);
            fail();
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("doesn't match the original one"));
        }

        // check and mute 只支持一行操作
        try {
            RowMutations mutations = new RowMutations("key_7".getBytes());
            Put put = new Put("key_7".getBytes());
            put.add("family1".getBytes(), "column1_1".getBytes(), "value2".getBytes());
            mutations.add(put);
            boolean ret = hTable.checkAndMutate("key_8".getBytes(), "family1".getBytes(),
                "column1_1".getBytes(), CompareFilter.CompareOp.EQUAL, "value1".getBytes(),
                mutations);
            fail();
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("mutation row is not equal check row error"));
        }

        try {
            Put put = new Put("key_8".getBytes());
            put.add("family2".getBytes(), "column1_1".getBytes(), "value2".getBytes());
            boolean ret = hTable.checkAndPut("key_8".getBytes(), "family1".getBytes(),
                "column1_1".getBytes(), "value1".getBytes(), put);
            fail();
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("mutation family is not equal check family"));
        }
    }

    @Test
    public void testCheckAndPut() throws IOException, InterruptedException {
        String key = "checkAndPutKey";
        String column = "checkAndPut";
        String value = "value";
        String family = "family1";
        Delete delete = new Delete(key.getBytes());
        delete.deleteFamily(family.getBytes());
        hTable.delete(delete);
        Get get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);
        Put put = new Put(key.getBytes());
        put.add(family.getBytes(), column.getBytes(), value.getBytes());
        hTable.put(put);
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        put = new Put(key.getBytes());
        put.add(family.getBytes(), column.getBytes(), "value1".getBytes());
        boolean ret = hTable.checkAndPut(key.getBytes(), "family1".getBytes(), column.getBytes(),
            value.getBytes(), put);
        Assert.assertTrue(ret);

        ret = hTable.checkAndPut(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.GREATER, "value1".getBytes(), put);
        Assert.assertFalse(ret);
        ret = hTable.checkAndPut(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.GREATER_OR_EQUAL, "value1".getBytes(), put);
        Assert.assertTrue(ret);
        ret = hTable.checkAndPut(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.LESS, "".getBytes(), put);
        Assert.assertFalse(ret);
        ret = hTable.checkAndPut(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.LESS_OR_EQUAL, "".getBytes(), put);
        Assert.assertFalse(ret);

        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(3, r.raw().length);
        Assert.assertEquals("value1", Bytes.toString(r.raw()[0].getValue()));
    }

    @Test
    public void testCheckAndDelete() throws IOException {
        // delete 只支持删一行
        String key = "checkAndDeleteKey";
        String column = "checkAndDeleteColumn";
        String column2 = "checkAndDeleteColumn2";
        String value = "value";
        String family = "family1";
        Delete delete = new Delete(key.getBytes());
        delete.deleteFamily(family.getBytes());
        hTable.delete(delete);
        Put put = new Put(key.getBytes());
        put.add(family.getBytes(), column.getBytes(), value.getBytes());
        hTable.put(put);

        // check delete column
        delete = new Delete(key.getBytes());
        delete.deleteColumn(family.getBytes(), column.getBytes());
        boolean ret = hTable.checkAndDelete(key.getBytes(), family.getBytes(), column.getBytes(),
            value.getBytes(), delete);
        Assert.assertTrue(ret);
        put = new Put(key.getBytes());
        put.add(family.getBytes(), column.getBytes(), "value6".getBytes());
        hTable.put(put);
        ret = hTable.checkAndDelete(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.GREATER, "value5".getBytes(), delete);
        Assert.assertTrue(ret);
        put = new Put(key.getBytes());
        put.add(family.getBytes(), column.getBytes(), "value5".getBytes());
        hTable.put(put);
        ret = hTable.checkAndDelete(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.GREATER_OR_EQUAL, "value5".getBytes(), delete);
        Assert.assertTrue(ret);
        put = new Put(key.getBytes());
        put.add(family.getBytes(), column.getBytes(), "value1".getBytes());
        hTable.put(put);
        ret = hTable.checkAndDelete(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.LESS, "value1".getBytes(), delete);
        Assert.assertFalse(ret);
        ret = hTable.checkAndDelete(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.LESS_OR_EQUAL, "value1".getBytes(), delete);
        Assert.assertTrue(ret);

        Get get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        // check delete columns
        long t = System.currentTimeMillis();
        put = new Put(key.getBytes());
        put.add(family.getBytes(), column.getBytes(), t, value.getBytes());
        put.add(family.getBytes(), column.getBytes(), t + 1, value.getBytes());
        hTable.put(put);
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);
        delete = new Delete(key.getBytes());
        delete.deleteColumns(family.getBytes(), column.getBytes());
        ret = hTable.checkAndDelete(key.getBytes(), family.getBytes(), column.getBytes(),
            value.getBytes(), delete);
        Assert.assertTrue(ret);
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        // check delete family
        t = System.currentTimeMillis();
        put = new Put(key.getBytes());
        put.add(family.getBytes(), column.getBytes(), t, value.getBytes());
        put.add(family.getBytes(), column.getBytes(), t + 1, value.getBytes());
        put.add(family.getBytes(), column2.getBytes(), t, value.getBytes());
        put.add(family.getBytes(), column2.getBytes(), t + 1, value.getBytes());
        hTable.put(put);
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addFamily(family.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(4, r.raw().length);
        delete = new Delete(key.getBytes());
        delete.deleteFamily(family.getBytes());
        ret = hTable.checkAndDelete(key.getBytes(), family.getBytes(), column.getBytes(),
            value.getBytes(), delete);
        Assert.assertTrue(ret);
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addFamily(family.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

    }

    @Test
    public void testCheckAndMutate() throws IOException {
        // Mutate 只支持操作一行数据
        String key = "checkAndMutateKey";
        String column1 = "checkAndMutateColumn";
        String column2 = "checkAndMutateColumn2";
        String value1 = "value1";
        String value2 = "value2";
        String family = "family1";
        Delete delete = new Delete(key.getBytes());
        delete.deleteFamily(family.getBytes());
        hTable.delete(delete);

        long t = System.currentTimeMillis();
        // put
        Put put1 = new Put(key.getBytes());
        put1.add(family.getBytes(), column1.getBytes(), t, value1.getBytes());
        put1.add(family.getBytes(), column2.getBytes(), t, value2.getBytes());

        Put put2 = new Put(key.getBytes());
        put2.add(family.getBytes(), column1.getBytes(), t + 3, value2.getBytes());
        put2.add(family.getBytes(), column2.getBytes(), t + 3, value1.getBytes());

        Put put3 = new Put(key.getBytes());
        put3.add(family.getBytes(), column1.getBytes(), t + 5, value1.getBytes());
        put3.add(family.getBytes(), column2.getBytes(), t + 5, value2.getBytes());

        RowMutations rowMutations = new RowMutations(key.getBytes());
        rowMutations.add(put1);
        rowMutations.add(put2);
        rowMutations.add(put3);

        //put data
        boolean ret = hTable.checkAndMutate(key.getBytes(),
            family.getBytes(StandardCharsets.UTF_8), column1.getBytes(),
            CompareFilter.CompareOp.EQUAL, null, rowMutations);

        Assert.assertTrue(ret);
        Get get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        Result r = hTable.get(get);
        Assert.assertEquals(6, r.raw().length);

        t = System.currentTimeMillis() + 7;
        // put
        put1 = new Put(key.getBytes());
        put1.add(family.getBytes(), column1.getBytes(), t, value1.getBytes());
        put1.add(family.getBytes(), column2.getBytes(), t, value2.getBytes());

        put2 = new Put(key.getBytes());
        put2.add(family.getBytes(), column1.getBytes(), t + 3, value2.getBytes());
        put2.add(family.getBytes(), column2.getBytes(), t + 3, value1.getBytes());

        put3 = new Put(key.getBytes());
        put3.add(family.getBytes(), column1.getBytes(), t + 5, value1.getBytes());
        put3.add(family.getBytes(), column2.getBytes(), t + 5, value2.getBytes());
        rowMutations = new RowMutations(key.getBytes());
        rowMutations.add(put1);
        rowMutations.add(put2);
        rowMutations.add(put3);
        // test greater op
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column1.getBytes(),
            CompareFilter.CompareOp.LESS, value1.getBytes(), rowMutations);
        Assert.assertFalse(ret);
        get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        r = hTable.get(get);
        Assert.assertEquals(6, r.raw().length);

        // test less op
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column1.getBytes(),
            CompareFilter.CompareOp.LESS, value2.getBytes(), rowMutations);
        Assert.assertTrue(ret);
        get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        r = hTable.get(get);
        Assert.assertEquals(12, r.raw().length);

        t = System.currentTimeMillis() + 14;
        // put
        put1 = new Put(key.getBytes());
        put1.add(family.getBytes(), column1.getBytes(), t, value1.getBytes());
        put1.add(family.getBytes(), column2.getBytes(), t, value2.getBytes());

        put2 = new Put(key.getBytes());
        put2.add(family.getBytes(), column1.getBytes(), t + 3, value2.getBytes());
        put2.add(family.getBytes(), column2.getBytes(), t + 3, value1.getBytes());

        put3 = new Put(key.getBytes());
        put3.add(family.getBytes(), column1.getBytes(), t + 5, value1.getBytes());
        put3.add(family.getBytes(), column2.getBytes(), t + 5, value2.getBytes());
        rowMutations = new RowMutations(key.getBytes());
        rowMutations.add(put1);
        rowMutations.add(put2);
        rowMutations.add(put3);
        // test NO_OP
        try {
            hTable.checkAndMutate(key.getBytes(), family.getBytes(), column1.getBytes(),
                CompareFilter.CompareOp.NO_OP, value1.getBytes(), rowMutations);
            fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("checkAndMutate"));
        }

        // test equal op
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column1.getBytes(),
            CompareFilter.CompareOp.EQUAL, value1.getBytes(), rowMutations);
        Assert.assertTrue(ret);
        get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        r = hTable.get(get);
        Assert.assertEquals(18, r.raw().length);

        t = System.currentTimeMillis() + 21;
        // put
        put1 = new Put(key.getBytes());
        put1.add(family.getBytes(), column1.getBytes(), t, value1.getBytes());
        put1.add(family.getBytes(), column2.getBytes(), t, value2.getBytes());

        // delete
        Delete delete1 = new Delete(key.getBytes());
        delete1.deleteColumns(family.getBytes(), column1.getBytes());

        // check delete and put
        rowMutations = new RowMutations(key.getBytes());
        rowMutations.add(delete1);
        rowMutations.add(put1);
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column1.getBytes(),
            CompareFilter.CompareOp.EQUAL, value1.getBytes(), rowMutations);
        Assert.assertTrue(ret);
        get = new Get(key.getBytes());
        get.addColumn(family.getBytes(), column1.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        get = new Get(key.getBytes());
        get.addColumn(family.getBytes(), column2.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        r = hTable.get(get);
        Assert.assertEquals(10, r.raw().length);
    }

    @Test
    public void testAppend() throws IOException {
        String column = "appendColumn";
        String key = "appendKey";
        Delete delete = new Delete(key.getBytes());
        delete.deleteColumns("family1".getBytes(), column.getBytes());
        hTable.delete(delete);

        // append an absent column is not supported yet
        //        Append append = new Append(key.getBytes());
        //        append.add("family1".getBytes(), column.getBytes(), toBytes("_append"));
        //        Result r = hTable.append(append);
        //        Assert.assertEquals(1, r.raw().length);
        //        for (KeyValue kv : r.raw()) {
        //            Assert.assertEquals("_append", Bytes.toString(kv.getValue()));
        //        }

        Put put = new Put(key.getBytes());
        put.add("family1".getBytes(), column.getBytes(), toBytes("value"));
        hTable.put(put);
        Append append = new Append(key.getBytes());
        append.add("family1".getBytes(), column.getBytes(), toBytes("_append"));
        Result r = hTable.append(append);
        Assert.assertEquals(1, r.raw().length);
        for (KeyValue kv : r.raw()) {
            Assert.assertEquals("value_append", Bytes.toString(kv.getValue()));
        }
    }

    @Test
    public void testIncrement() throws IOException {
        String column = "incrementColumn";
        String key = "incrementKey";
        Delete delete = new Delete(key.getBytes());
        delete.deleteColumns("family1".getBytes(), column.getBytes());
        hTable.delete(delete);

        // increment an absent column is not supported yet
        //        Increment increment = new Increment(key.getBytes());
        //        increment.addColumn("family1".getBytes(), column.getBytes(), 1);
        //        Result r = hTable.increment(increment);
        //        Assert.assertEquals(1, r.raw().length);
        //        for (KeyValue kv : r.raw()) {
        //            Assert.assertEquals(1L, Bytes.toLong(kv.getValue()));
        //        }

        long timestamp = System.currentTimeMillis();
        Put put = new Put(key.getBytes());
        put.add("family1".getBytes(), column.getBytes(), timestamp, toBytes(1L));
        put.add("family1".getBytes(), column.getBytes(), timestamp + 10, toBytes(1L));
        hTable.put(put);
        Increment increment = new Increment(key.getBytes());
        increment.addColumn("family1".getBytes(), column.getBytes(), 1);
        Result r = hTable.increment(increment);
        Assert.assertEquals(1, r.raw().length);
        for (KeyValue kv : r.raw()) {
            Assert.assertEquals(2L, Bytes.toLong(kv.getValue()));
        }

        // increment an absent column is not supported yet
        //        increment = new Increment(key.getBytes());
        //        increment.addColumn("family1".getBytes(), column.getBytes(), 1);
        //        increment.setTimeRange(timestamp + 3, timestamp + 8);
        //        r = hTable.increment(increment);
        //        Assert.assertEquals(1, r.raw().length);
        //        for (KeyValue kv : r.raw()) {
        //            Assert.assertEquals(1L, Bytes.toLong(kv.getValue()));
        //        }

        long ret = hTable.incrementColumnValue(key.getBytes(), "family1".getBytes(),
            column.getBytes(), 1);
        Assert.assertEquals(3, ret);
        ret = hTable.incrementColumnValue(key.getBytes(), "family1".getBytes(), column.getBytes(),
            1L, null);
        Assert.assertEquals(4, ret);
    }

    @Test
    public void testExist() throws IOException {
        String column = "existColumn";
        String key = "existKey";
        Delete delete = new Delete(key.getBytes());
        delete.deleteColumns("family1".getBytes(), column.getBytes());
        hTable.delete(delete);

        Get get = new Get(key.getBytes());
        get.addFamily("family1".getBytes());
        Assert.assertFalse(hTable.exists(get));

        long timestamp = System.currentTimeMillis();
        Put put = new Put(key.getBytes());
        put.add("family1".getBytes(), column.getBytes(), timestamp, "value".getBytes());
        hTable.put(put);

        get = new Get(key.getBytes());
        get.addFamily("family1".getBytes());
        Assert.assertTrue(hTable.exists(get));

        get = new Get(key.getBytes());
        get.addColumn("family1".getBytes(), column.getBytes());
        Assert.assertTrue(hTable.exists(get));

        get.setTimeStamp(timestamp);
        Assert.assertTrue(hTable.exists(get));

        get.setTimeStamp(timestamp + 1);
        Assert.assertFalse(hTable.exists(get));

        hTable.delete(delete);
    }

    @Ignore
    public void testMutateRow() throws IOException {

        String column1 = "mutationRowColumn1";
        String column2 = "mutationRowColumn2";
        String key = "mutationRowKey";
        String family1 = "family1";
        String family2 = "family2";
        String value = "value";

        Delete deleteFamily = new Delete(key.getBytes());
        deleteFamily.deleteFamily(toBytes(family1));

        Get family1Get = new Get(toBytes(key));
        family1Get.addFamily(toBytes(family1));

        Delete deleteColumn1 = new Delete(key.getBytes());
        deleteColumn1.deleteColumns(toBytes(family1), toBytes(column1));

        Delete deleteColumn2 = new Delete(key.getBytes());
        deleteColumn2.deleteColumns(toBytes(family1), toBytes(column2));

        Put putColumn1 = new Put(toBytes(key));
        putColumn1.add(toBytes(family1), toBytes(column1), toBytes(value));

        Put putColumn2 = new Put(toBytes(key));
        putColumn2.add(toBytes(family1), toBytes(column2), toBytes(value));

        hTable.delete(deleteFamily);
        RowMutations rm1 = new RowMutations(toBytes(key));
        rm1.add(putColumn1);
        hTable.mutateRow(rm1);
        Result r1 = hTable.get(family1Get);
        Assert.assertEquals(1, r1.raw().length);
        KeyValue r1KV = r1.raw()[0];
        Assert.assertEquals(key, Bytes.toString(r1KV.getRow()));
        Assert.assertEquals(family1, Bytes.toString(r1KV.getFamily()));
        Assert.assertEquals(column1, Bytes.toString(r1KV.getQualifier()));
        Assert.assertEquals(value, Bytes.toString(r1KV.getValue()));

        hTable.delete(deleteFamily);
        RowMutations rm2 = new RowMutations(toBytes(key));
        rm2.add(putColumn2);
        rm2.add(putColumn1);
        hTable.mutateRow(rm2);
        Result r2 = hTable.get(family1Get);
        Assert.assertEquals(2, r2.raw().length);
        hTable.delete(deleteFamily);

        hTable.put(putColumn1);
        RowMutations rm3 = new RowMutations(toBytes(key));
        rm3.add(deleteColumn1);
        hTable.mutateRow(rm3);
        Result r3 = hTable.get(family1Get);
        Assert.assertEquals(0, r3.raw().length);

        hTable.put(putColumn1);
        hTable.put(putColumn2);
        RowMutations rm4 = new RowMutations(toBytes(key));
        rm4.add(deleteColumn1);
        rm4.add(deleteColumn2);
        hTable.mutateRow(rm4);
        Result r4 = hTable.get(family1Get);
        Assert.assertEquals(0, r4.raw().length);

        hTable.put(putColumn1);
        RowMutations rm5 = new RowMutations(toBytes(key));
        rm5.add(putColumn2);
        rm5.add(deleteColumn1);
        hTable.mutateRow(rm5);
        Result r5 = hTable.get(family1Get);
        Assert.assertEquals(1, r5.raw().length);
        KeyValue r5KV = r5.raw()[0];
        Assert.assertEquals(key, Bytes.toString(r5KV.getRow()));
        Assert.assertEquals(family1, Bytes.toString(r5KV.getFamily()));
        Assert.assertEquals(column2, Bytes.toString(r5KV.getQualifier()));
        Assert.assertEquals(value, Bytes.toString(r5KV.getValue()));
        hTable.delete(deleteFamily);
    }

    @Test
    public void testQualifyNull() throws Exception {
        // delete 只支持删一行
        String key = "qualifyNullKey";
        String value = "value";
        String value1 = "value1";
        String family = "family1";
        Delete delete = new Delete(key.getBytes());
        delete.deleteFamily(family.getBytes());
        hTable.delete(delete);
        Put put = new Put(key.getBytes());
        put.add(family.getBytes(), null, value.getBytes());
        hTable.put(put);

        Get get = new Get(key.getBytes());
        get.addColumn(Bytes.toBytes(family), null);
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);
        Assert.assertEquals(key, Bytes.toString(r.raw()[0].getRow()));
        Assert.assertEquals(value, Bytes.toString(r.raw()[0].getValue()));
        Assert.assertEquals(0, r.raw()[0].getQualifier().length);

        put = new Put(key.getBytes());
        put.add(family.getBytes(), null, value1.getBytes());

        hTable.checkAndPut(Bytes.toBytes(key), Bytes.toBytes(family), null, Bytes.toBytes(value),
            put);

        get = new Get(key.getBytes());
        get.addColumn(Bytes.toBytes(family), null);
        r = hTable.get(get);
        for (KeyValue kv : r.raw()) {
            System.out.println("K = [" + Bytes.toString(kv.getRow()) + "] Q =["
                               + Bytes.toString(kv.getQualifier()) + "] T = [" + kv.getTimestamp()
                               + "] V = [" + Bytes.toString(kv.getValue()) + "]");
        }
        Assert.assertEquals(1, r.raw().length);
        Assert.assertEquals(key, Bytes.toString(r.raw()[0].getRow()));
        Assert.assertEquals(value1, Bytes.toString(r.raw()[0].getValue()));
        Assert.assertEquals(0, r.raw()[0].getQualifier().length);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), null);
        scan.setMaxVersions(1);
        scan.setStartRow("qualifyNullKey".getBytes());
        scan.setStopRow("qualifyNullKeyA".getBytes());
        List<KeyValue> resultList = new ArrayList<KeyValue>();
        ResultScanner scanner = hTable.getScanner(scan);

        while ((r = scanner.next()) != null) {
            resultList.addAll(Arrays.asList(r.raw()));
        }
        Assert.assertEquals(1, resultList.size());

        scan.setMaxVersions(10);
        resultList = new ArrayList<KeyValue>();
        scanner = hTable.getScanner(scan);

        while ((r = scanner.next()) != null) {
            resultList.addAll(Arrays.asList(r.raw()));
        }

        Assert.assertEquals(2, resultList.size());

        Delete deleteColumn = new Delete(key.getBytes());
        deleteColumn.deleteColumn(Bytes.toBytes(family), null);
        hTable.delete(deleteColumn);

        get = new Get(key.getBytes());
        get.addColumn(Bytes.toBytes(family), null);
        r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        Delete deleteColumns = new Delete(key.getBytes());
        deleteColumns.deleteColumns(Bytes.toBytes(family), null);
        hTable.delete(deleteColumns);

        get = new Get(key.getBytes());
        get.addColumn(Bytes.toBytes(family), null);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);
    }

    @Test
    public void testFamilyBlank() throws Exception {
        // delete 只支持删一行
        String key = "qualifyNullKey";
        String value = "value";
        String value1 = "value1";
        String family = "   ";
        Delete delete = new Delete(key.getBytes());
        delete.deleteFamily(family.getBytes());
        try {
            hTable.delete(delete);
            fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("family is blank"));
        }
        Put put = new Put(key.getBytes());
        put.add(null, null, value.getBytes());
        try {
            hTable.put(put);
            fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("family is empty"));
        }

        Append append = new Append(key.getBytes());
        // append.add(null, null, null);
        try {
            hTable.append(append);
            fail();
        } catch (FeatureNotSupportedException e) {
            Assert.assertTrue(e.getMessage().contains("family is empty"));
        }

        Increment increment = new Increment(key.getBytes());
        // increment.addColumn(null, null, 1);
        try {
            hTable.increment(increment);
            fail();
        } catch (FeatureNotSupportedException e) {
            Assert.assertTrue(e.getMessage().contains("family is empty"));
        }
    }

    @Test
    public void testScannerMultiVersion() throws Exception {
        // delete 只支持删一行
        String key = "scannerMultiVersion1";
        String value = "value";
        String column = "column";
        String value1 = "value1";
        String family = "family1";
        Delete delete = new Delete(key.getBytes());
        delete.deleteFamily(family.getBytes());
        hTable.delete(delete);
        Put put = new Put(key.getBytes());
        put.add(family.getBytes(), Bytes.toBytes(column), value.getBytes());
        hTable.put(put);

        Get get = new Get(key.getBytes());
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);
        Assert.assertEquals(key, Bytes.toString(r.raw()[0].getRow()));
        Assert.assertEquals(value, Bytes.toString(r.raw()[0].getValue()));

        put = new Put(key.getBytes());
        put.add(family.getBytes(), Bytes.toBytes(column), value1.getBytes());

        hTable.checkAndPut(Bytes.toBytes(key), Bytes.toBytes(family), Bytes.toBytes(column),
            Bytes.toBytes(value), put);

        get = new Get(key.getBytes());
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);
        Assert.assertEquals(key, Bytes.toString(r.raw()[0].getRow()));
        Assert.assertEquals(value1, Bytes.toString(r.raw()[0].getValue()));

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        scan.setMaxVersions(1);
        scan.setStartRow(("scannerMultiVersion1").getBytes());
        scan.setStopRow(("scannerMultiVersion9").getBytes());
        List<KeyValue> resultList = new ArrayList<KeyValue>();
        ResultScanner scanner = hTable.getScanner(scan);

        while ((r = scanner.next()) != null) {
            resultList.addAll(Arrays.asList(r.raw()));
        }
        Assert.assertEquals(1, resultList.size());

        scan.setMaxVersions(10);
        resultList = new ArrayList<KeyValue>();
        scanner = hTable.getScanner(scan);

        while ((r = scanner.next()) != null) {
            resultList.addAll(Arrays.asList(r.raw()));
        }

        Assert.assertEquals(2, resultList.size());
    }

    @Test
    public void testPutColumnFamilyNull() throws Exception {
        Put put1 = new Put(("key_c_f").getBytes());
        put1.add(null, ("column1").getBytes(), "value1_family_null".getBytes());
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("family is empty");
        hTable.put(put1);
    }

    @Test
    public void testPutColumnFamilyEmpty() throws Exception {
        Put put2 = new Put(("key_c_f").getBytes());
        put2.add("".getBytes(), ("column1").getBytes(), "value1_family_empty".getBytes());
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("family is empty");
        hTable.put(put2);
    }

    @Test
    public void testPutColumnFamilySpace() throws Exception {
        Put put3 = new Put(("key_c_f").getBytes());
        put3.add("  ".getBytes(), ("column1").getBytes(), "value1_family_space".getBytes());
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("family is blank");
        hTable.put(put3);
    }

    @Test
    public void testPutColumnFamilyNotExists() throws Exception {
        /** family 不存在时提示不友好，*/
        Put put4 = new Put(("key_c_f").getBytes());
        put4.add("family_not_exists".getBytes(), "column2".getBytes(),
            System.currentTimeMillis() - 1000 * 60 * 60L, "now - 1h".getBytes());
        expectedException.expect(IOException.class);
        hTable.put(put4);
    }

    @Test
    public void testGetColumnFamilyNull() throws Exception {
        Get get = new Get(("key_c_f").getBytes());
        expectedException.expect(NullPointerException.class);
        get.addFamily(null);
        expectedException.expect(FeatureNotSupportedException.class);
        expectedException.expectMessage("family is empty");
        hTable.get(get);
    }

    @Test
    public void testGetColumnFamilySpace() throws Exception {
        Get get = new Get(("key_c_f").getBytes());
        get.addFamily("  ".getBytes());
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("family is blank");
        hTable.get(get);
    }

    @Test
    public void testGetColumnFamilyNotExists() throws Exception {
        /** family 不存在时提示不友好，*/
        Get get = new Get(("key_c_f").getBytes());
        get.addFamily("family_not_exists".getBytes());
        expectedException.expect(IOException.class);
        expectedException.expectMessage("query table:test family family_not_exists error.");
        hTable.get(get);
    }

    @Test
    public void testColumnQualifier() throws Exception {
        Put put = new Put(("key_c_q_null").getBytes());
        put.add("family1".getBytes(), null, "value1_qualifier_null".getBytes());
        hTable.put(put);
        Get get = new Get("key_c_q_null".getBytes());
        get.addColumn("family1".getBytes(), null);
        //get.setMaxVersions();
        Result result = hTable.get(get);
        Assert.assertEquals("", 1, result.raw().length);
        Assert.assertEquals("", 0, result.raw()[0].getQualifierLength());
        Assert
            .assertArrayEquals("", "value1_qualifier_null".getBytes(), result.raw()[0].getValue());

        put = new Put(("key_c_q_empty").getBytes());
        put.add("family1".getBytes(), "".getBytes(), "value1_qualifier_empty".getBytes());
        hTable.put(put);
        get = new Get("key_c_q_empty".getBytes());
        get.addColumn("family1".getBytes(), "".getBytes());
        result = hTable.get(get);
        Assert.assertEquals("", 1, result.raw().length);
        Assert.assertEquals("", 0, result.raw()[0].getQualifierLength());
        Assert.assertArrayEquals("", "value1_qualifier_empty".getBytes(),
            result.raw()[0].getValue());

        put = new Put(("key_c_q_space").getBytes());
        put.add("family1".getBytes(), "  ".getBytes(), "value1_qualifier_space".getBytes());
        hTable.put(put);
        get = new Get("key_c_q_space".getBytes());
        get.addColumn("family1".getBytes(), "  ".getBytes());
        result = hTable.get(get);
        Assert.assertEquals("", 1, result.raw().length);
        Assert.assertArrayEquals("", "value1_qualifier_space".getBytes(),
            result.raw()[0].getValue());
        //空格可以做qualifier
        Assert.assertEquals("", 2, result.raw()[0].getQualifierLength());

        get = new Get("key_c_q_space".getBytes());
        get.addColumn("family1".getBytes(), " ".getBytes());
        result = hTable.get(get);
        //一个空格的qualifier不能取出两个空格的qualifier的值
        Assert.assertEquals("", 0, result.raw().length);

        get = new Get("key_c_q_space".getBytes());
        get.addColumn("family1".getBytes(), "".getBytes());
        result = hTable.get(get);
        //empty字符串qualifier不能取出两个空格的qualifier的值
        Assert.assertEquals("", 0, result.raw().length);

    }

    //    @Test
    public void testTTLColumnLevel() throws Exception {
        Put put = new Put(("key_ttl_column").getBytes());
        put.add("family_ttl".getBytes(), ("column1").getBytes(),
            "column1_value1_ttl_column".getBytes());
        put.add("family_ttl".getBytes(), ("column2").getBytes(),
            "column2_value1_ttl_column".getBytes());
        hTable.put(put);
        Get get = new Get("key_ttl_column".getBytes());
        get.addColumn("family_ttl".getBytes(), "column1".getBytes());
        get.addColumn("family_ttl".getBytes(), "column2".getBytes());
        get.setMaxVersions(1);
        Result result = hTable.get(get);
        Assert.assertEquals("", 2, result.raw().length);
        Assert.assertEquals("", 1, result
            .getColumn("family_ttl".getBytes(), ("column1").getBytes()).size());
        Assert.assertEquals("", 1, result
            .getColumn("family_ttl".getBytes(), ("column2").getBytes()).size());
        Assert.assertArrayEquals("", "column1_value1_ttl_column".getBytes(),
            result.getColumn("family_ttl".getBytes(), ("column1").getBytes()).get(0).getValue());
        Assert.assertArrayEquals("", "column2_value1_ttl_column".getBytes(),
            result.getColumn("family_ttl".getBytes(), ("column2").getBytes()).get(0).getValue());

        //过期之后不能再查出数据
        Thread.sleep(4 * 1000L);
        result = hTable.get(get);
        Assert.assertEquals("", 0, result.raw().length);
    }

    //    @Test
    public void testTTLFamilyLevel() throws Exception {
        Put put = new Put(("key_ttl_family").getBytes());
        put.add("family_ttl".getBytes(), ("column1").getBytes(),
            "column1_value_ttl_family".getBytes());
        put.add("family_ttl".getBytes(), ("column2").getBytes(),
            "column2_value_ttl_family".getBytes());
        hTable.put(put);
        Get get = new Get("key_ttl_family".getBytes());
        get.addFamily("family_ttl".getBytes());
        get.setMaxVersions(1);
        Result result = hTable.get(get);
        Assert.assertEquals("", 2, result.raw().length);
        Assert.assertEquals("", 1, result
            .getColumn("family_ttl".getBytes(), ("column1").getBytes()).size());
        Assert.assertEquals("", 1, result
            .getColumn("family_ttl".getBytes(), ("column2").getBytes()).size());
        Assert.assertArrayEquals("", "column1_value_ttl_family".getBytes(),
            result.getColumn("family_ttl".getBytes(), ("column1").getBytes()).get(0).getValue());
        Assert.assertArrayEquals("", "column2_value_ttl_family".getBytes(),
            result.getColumn("family_ttl".getBytes(), ("column2").getBytes()).get(0).getValue());

        //过期之后不能再查出数据
        Thread.sleep(4 * 1000L);
        result = hTable.get(get);
        Assert.assertEquals("", 0, result.raw().length);
    }

    public class IncrementHelper implements Runnable {
        private Table     hTable;
        private Increment increment;

        public IncrementHelper(Table hTable, Increment increment) {
            this.hTable = hTable;
            this.increment = increment;
        }

        @Override
        public void run() {
            try {
                hTable.increment(increment);
            } catch (Exception e) {
                e.getMessage();
            }
        }
    }

    public class PutHelper implements Runnable {
        private Table hTable;
        private Put   put;

        public PutHelper(Table hTable, Put put) {
            this.hTable = hTable;
            this.put = put;
        }

        @Override
        public void run() {
            try {
                hTable.put(put);
            } catch (Exception e) {
                e.getMessage();
            }
        }
    }

    public static byte[] toByteArray(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    // increment并发结果的正确性测试
    @Test
    public void testIncrementConcurrency() throws Exception {
        String column = "incrementColumn";
        String key = "incrementKey";
        Delete delete = new Delete(key.getBytes());
        delete.deleteColumns("family1".getBytes(), column.getBytes());
        hTable.delete(delete);

        for (int i = 0; i < 100; i++) {
            Increment increment = new Increment(key.getBytes());
            increment.addColumn("family1".getBytes(), column.getBytes(), 1);
            Thread t = new Thread(new IncrementHelper(hTable, increment));
            t.start();
        }
        Thread.sleep(300);
        Put put = new Put(key.getBytes());
        put.add("family1".getBytes(), column.getBytes(), toByteArray(1));
        Thread t = new Thread(new PutHelper(hTable, put));
        t.start();
        t.join();

        Thread.sleep(8000);

        Get get = new Get("incrementKey".getBytes());
        get.addColumn("family1".getBytes(), "incrementColumn".getBytes());
        Result result = hTable.get(get);
        KeyValue kv = result.raw()[0];
        System.out.println(Bytes.toLong(kv.getValue()));
        Assert.assertTrue(Bytes.toLong(kv.getValue()) < 100);

        Thread.sleep(2000);
        get.setMaxVersions(200);
        result = hTable.get(get);
        assertEquals(101, result.raw().length);
    }

    // Test operation in hbase table with local index
    @Test
    public void testHtableWithIndex() throws Exception {
        testBasic("family_with_local_index");
    }

    @Test
    public void testFilterSpecialValue() throws IOException {
        // 1. test checkAndMutate with special character
        String specialValue = "AAEAAAGRnJiQbwAAAZGcmJwndjE=";
        byte[] specialBytes = Base64.getDecoder().decode(specialValue);
        String family = "family'1";
        byte[] keyBytes = specialBytes;
        byte[] columnBytes = specialBytes;
        byte[] valueBytes = specialBytes;

        Delete delete = new Delete(keyBytes);
        delete.deleteFamily(family.getBytes());
        hTable.delete(delete);

        Put put = new Put(keyBytes);
        put.add(family.getBytes(), columnBytes, valueBytes);
        hTable.put(put);

        // check delete column
        delete = new Delete(keyBytes);
        delete.deleteColumn(family.getBytes(), columnBytes);
        boolean ret = hTable.checkAndDelete(keyBytes, family.getBytes(), columnBytes, valueBytes,
            delete);
        Assert.assertTrue(ret);

        // 2. test normal filter with special chracter
        put = new Put(keyBytes);
        put.add(family.getBytes(), columnBytes, valueBytes);
        hTable.put(put);

        Get get = new Get(keyBytes);
        get.addFamily(family.getBytes());
        // 2.1 test special row
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            keyBytes));
        get.setFilter(rowFilter);
        Result result = hTable.get(get);
        Assert.assertEquals(1, result.raw().length);
        Assert.assertArrayEquals(keyBytes, result.getRow());

        // 2.2 test special column
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
            family.getBytes(), columnBytes, CompareFilter.CompareOp.EQUAL, valueBytes);
        get.setFilter(singleColumnValueFilter);
        result = hTable.get(get);
        Assert.assertEquals(1, result.raw().length);
        Assert.assertArrayEquals(keyBytes, result.getRow());

        // 2.3 test special value
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(valueBytes));
        get.setFilter(valueFilter);
        result = hTable.get(get);
        Assert.assertEquals(1, result.raw().length);
        Assert.assertArrayEquals(keyBytes, result.getRow());

    }
}
