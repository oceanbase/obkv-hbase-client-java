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
import com.alipay.oceanbase.rpc.exception.ObTableNotExistException;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ONE;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class HTableTestBase {

    @Rule
    public ExpectedException  expectedException = ExpectedException.none();

    protected HTableInterface hTable;

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
            System.out.println("rowKey: " + new String(keyValue.getRow())
                    + " family :" + new String(keyValue.getFamily())
                    + " columnQualifier:"
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
            System.out.println("rowKey: " + new String(keyValue.getRow())
                    + " family :" + new String(keyValue.getFamily())
                    + " columnQualifier:"
                    + new String(keyValue.getQualifier()) + " timestamp:"
                    + keyValue.getTimestamp() + " value:"
                    + new String(keyValue.getValue()));
        }

        // test scan with empty family
        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                System.out.println("rowKey: " + new String(keyValue.getRow())
                                   + " family :" + new String(keyValue.getFamily())
                                   + " columnQualifier:"
                                   + new String(keyValue.getQualifier()) + " timestamp:"
                                   + keyValue.getTimestamp() + " value:"
                                   + new String(keyValue.getValue()));
            }
        }
    }

    @Test
    public void testBasic() throws Exception {
        String key = "putKey";
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        String value = "value";
        String family = "family1";
        long timestamp = System.currentTimeMillis();
        Delete delete = new Delete(key.getBytes());
        delete.deleteFamily(family.getBytes());
        hTable.delete(delete);

        Put put = new Put(toBytes(key));
        put.add("family1".getBytes(), column1.getBytes(), timestamp, toBytes(value));
        hTable.put(put);
        Get get = new Get(toBytes(key));
        get.addColumn("family1".getBytes(), toBytes(column1));
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        for (KeyValue keyValue : r.raw()) {
            System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                               + new String(keyValue.getQualifier()) + " timestamp:"
                               + keyValue.getTimestamp() + " value:"
                               + new String(keyValue.getValue()));
            Assert.assertEquals(column1, Bytes.toString(keyValue.getQualifier()));
            Assert.assertEquals(value, Bytes.toString(keyValue.getValue()));
        }

        put = new Put(toBytes(key));
        put.add("family1".getBytes(), column1.getBytes(), timestamp + 1, toBytes(value));
        hTable.put(put);
        get = new Get(toBytes(key));
        get.addColumn("family1".getBytes(), toBytes(column1));
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
            Assert.assertEquals(column1, Bytes.toString(keyValue.getQualifier()));
            Assert.assertEquals(value, Bytes.toString(keyValue.getValue()));
        }

        try {
            for (int j = 0; j < 10; j++) {
                put = new Put((key + "_" + j).getBytes());
                put.add("family1".getBytes(), column1.getBytes(), toBytes(value));
                put.add("family1".getBytes(), column2.getBytes(), toBytes(value));
                hTable.put(put);
            }

            Scan scan = new Scan();
            scan.addColumn("family1".getBytes(), column1.getBytes());
            scan.addColumn("family1".getBytes(), column2.getBytes());
            scan.setStartRow(toBytes(key + "_" + 0));
            scan.setStopRow(toBytes(key + "_" + 9));
            scan.setMaxVersions(9);
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
        List<Put> puts = new ArrayList<Put>();
        puts.add(put1);
        puts.add(put2);
        hTable.put(puts);

        // put same k, q, t
        Put put3 = new Put(Bytes.toBytes("testKey"));
        put3.add(toBytes(family), toBytes(column1), -100L, toBytes(value1));
        put3.add(toBytes(family), toBytes(column1), -100L, toBytes(value1));

        Put put4 = new Put(Bytes.toBytes("testKey"));
        put4.add(toBytes(family), toBytes(column1), System.currentTimeMillis(), toBytes(value1));
        put4.add(toBytes(family), toBytes(column1), System.currentTimeMillis(), toBytes(value1));
        puts = new ArrayList<Put>();
        puts.add(put3);
        puts.add(put4);
        hTable.put(puts);
    }

    public void tryPut(HTableInterface hTable, Put put) throws Exception {
        hTable.put(put);
        Thread.sleep(1);
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
    public void testScan() throws Exception {
        String key1 = "scanKey1x";
        String key2 = "scanKey2x";
        String key3 = "scanKey3x";
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

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);


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
        scan.addFamily("family1".getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey2x".getBytes());
        scan.setMaxVersions(10);
        ResultScanner scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                Arrays.equals(key1.getBytes(), keyValue.getRow());
                res_count+=1;
            }
        }
        Assert.assertEquals(res_count, 7);

        // scan with prefixFilter
        scan = new Scan();
        scan.addFamily("family1".getBytes());
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
                res_count+=1;
            }
        }
        Assert.assertEquals(res_count, 2);

        // scan with singleColumnValueFilter
        // 任何一个版本满足则返回本行
        SingleColumnValueFilter singleColumnValueFilter;
        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
                Bytes.toBytes(column1), CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value1)));
        scan = new Scan();
        scan.addFamily("family1".getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey3x".getBytes());
        scan.setFilter(singleColumnValueFilter);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                res_count+=1;
            }
        }
        Assert.assertEquals(res_count, 9);

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
    }

    @Test
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
            Assert.assertTrue(e.getCause().getMessage()
                .contains("mutation row is not equal check row"));
        }

        try {
            Put put = new Put("key_8".getBytes());
            put.add("family2".getBytes(), "column1_1".getBytes(), "value2".getBytes());
            boolean ret = hTable.checkAndPut("key_8".getBytes(), "family1".getBytes(),
                "column1_1".getBytes(), "value1".getBytes(), put);
            fail();
        } catch (IOException e) {
            Assert.assertTrue(e.getCause().getMessage()
                .contains("mutation family is not equal check family"));
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
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);
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
            1, true);
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
    public void testFamilyNull() throws Exception {
        // delete 只支持删一行
        String key = "qualifyNullKey";
        String value = "value";
        String value1 = "value1";
        String family = null;
        Delete delete = new Delete(key.getBytes());
        delete.deleteFamily("   ".getBytes());
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
            Assert.assertTrue(e.getMessage().contains("family is blank"));
        }

        Get get = new Get(key.getBytes());
        get.addColumn(Bytes.toBytes(""), null);
        Result r = null;
        try {
            r = hTable.get(get);
            fail();
        } catch (FeatureNotSupportedException e) {
            Assert.assertTrue(e.getMessage().contains("family is empty"));
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("family is blank"));
        }

        Scan scan = new Scan(key.getBytes());
        scan.addColumn(Bytes.toBytes(""), null);
        try {
            hTable.getScanner(scan);
            fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("family is blank"));
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
        expectedException.expectMessage("family is blank");
        hTable.put(put1);
    }

    @Test
    public void testPutColumnFamilyEmpty() throws Exception {
        Put put2 = new Put(("key_c_f").getBytes());
        put2.add("".getBytes(), ("column1").getBytes(), "value1_family_empty".getBytes());
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("family is blank");
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
    public void testGetColumnFamilyEmpty() throws Exception {
        Get get = new Get(("key_c_f").getBytes());
        get.addFamily("".getBytes());
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("family is blank");
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
        private HTableInterface hTable;
        private Increment       increment;

        public IncrementHelper(HTableInterface hTable, Increment increment) {
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
        private HTableInterface hTable;
        private Put             put;

        public PutHelper(HTableInterface hTable, Put put) {
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
}
