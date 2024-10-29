/*-
 * #%L
 * com.oceanbase:obkv-hbase-client
 * %%
 * Copyright (C) 2022 - 2024 OceanBase Group
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

import com.alipay.oceanbase.hbase.util.OHBufferedMutatorImpl;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import org.apache.hadoop.conf.Configuration;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hbase.ipc.RpcClient.SOCKET_TIMEOUT_CONNECT;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.*;

public abstract class HTableMultiCFTestBase {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected static Table   multiCfHTable;

    public void tryPut(Table multiCfHTable, Put put) throws Exception {
        multiCfHTable.put(put);
        Thread.sleep(1);
    }

    @Test
    public void testDeleteFamilyVerison() throws Exception {
        String key1 = "scanKey1x";
        String key2 = "scanKey2x";
        String key3 = "scanKey3x";
        String column1 = "column1";
        String column2 = "column2";
        String column3 = "column3";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";
        String family1 = "family_with_group1";
        String family2 = "family_with_group2";
        // delete previous data
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.addFamily(toBytes(family1));
        deleteKey1Family.addFamily(toBytes(family2));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family1));
        deleteKey2Family.addFamily(toBytes(family2));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.addFamily(toBytes(family1));
        deleteKey3Family.addFamily(toBytes(family2));

        multiCfHTable.delete(deleteKey1Family);
        multiCfHTable.delete(deleteKey2Family);
        multiCfHTable.delete(deleteKey3Family);

        long minTimeStamp = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp1 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp2 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp3 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp4 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp5 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp6 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp7 = System.currentTimeMillis();
        Thread.sleep(5);

        Put putKey1Fam1Column1MinTs = new Put(toBytes(key1));
        putKey1Fam1Column1MinTs.addColumn(toBytes(family1), toBytes(column1), minTimeStamp,
            toBytes(value1));

        Put putKey3Fam1Column1Ts1 = new Put(toBytes(key3));
        putKey3Fam1Column1Ts1.addColumn(toBytes(family1), toBytes(column1), timeStamp1,
            toBytes(value2));

        Put putKey1Fam1Column2MinTs = new Put(toBytes(key1));
        putKey1Fam1Column2MinTs.addColumn(toBytes(family1), toBytes(column2), minTimeStamp,
            toBytes(value1));

        Put putKey1Fam1Column2Ts3 = new Put(toBytes(key1));
        putKey1Fam1Column2Ts3.addColumn(toBytes(family1), toBytes(column2), timeStamp3,
            toBytes(value2));

        Put putKey2Fam1Column2Ts3 = new Put(toBytes(key2));
        putKey2Fam1Column2Ts3.addColumn(toBytes(family1), toBytes(column2), timeStamp3,
            toBytes(value2));

        Put putKey2Fam1Column3Ts1 = new Put(toBytes(key2));
        putKey2Fam1Column3Ts1.addColumn(toBytes(family1), toBytes(column3), timeStamp1,
            toBytes(value2));

        Put putKey3Fam1Column3Ts1 = new Put(toBytes(key3));
        putKey3Fam1Column3Ts1.addColumn(toBytes(family1), toBytes(column3), timeStamp1,
            toBytes(value2));

        Put putKey3Fam1Column2Ts4 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts4.addColumn(toBytes(family1), toBytes(column2), timeStamp4,
            toBytes(value1));

        Put putKey2Fam1Column3Ts3 = new Put(toBytes(key2));
        putKey2Fam1Column3Ts3.addColumn(toBytes(family1), toBytes(column3), timeStamp3,
            toBytes(value1));

        tryPut(multiCfHTable, putKey1Fam1Column1MinTs);
        tryPut(multiCfHTable, putKey3Fam1Column1Ts1);
        tryPut(multiCfHTable, putKey1Fam1Column2MinTs);
        tryPut(multiCfHTable, putKey1Fam1Column2Ts3);
        tryPut(multiCfHTable, putKey2Fam1Column2Ts3);
        tryPut(multiCfHTable, putKey2Fam1Column3Ts1);
        tryPut(multiCfHTable, putKey3Fam1Column3Ts1);
        tryPut(multiCfHTable, putKey3Fam1Column2Ts4);
        tryPut(multiCfHTable, putKey2Fam1Column3Ts3);

        // test DeleteFamilyVersion single cf
        Get get = new Get(toBytes(key1));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(minTimeStamp);
        get.setMaxVersions(10);
        Result r = multiCfHTable.get(get);
        Assert.assertEquals(2, r.getRow().length);

        get = new Get(toBytes(key3));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(timeStamp1);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(2, r.getRow().length);

        get = new Get(toBytes(key2));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(timeStamp3);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(2, r.getRow().length);

        Delete delKey1MinTs = new Delete(toBytes(key1));
        delKey1MinTs.addFamilyVersion(toBytes(family1), minTimeStamp);
        multiCfHTable.delete(delKey1MinTs);

        get = new Get(toBytes(key1));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(minTimeStamp);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.getRow().length);

        Delete delKey3Ts1 = new Delete(toBytes(key3));
        delKey3Ts1.addFamilyVersion(toBytes(family1), timeStamp1);
        multiCfHTable.delete(delKey3Ts1);

        get = new Get(toBytes(key3));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(timeStamp1);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.getRow().length);

        Delete delKey2Ts3 = new Delete(toBytes(key2));
        delKey2Ts3.addFamilyVersion(family1.getBytes(), timeStamp3);
        multiCfHTable.delete(delKey2Ts3);

        get = new Get(toBytes(key2));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(timeStamp3);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.getRow().length);

        Scan scan = new Scan();
        scan.setStartRow(toBytes(key1));
        scan.setStopRow("scanKey4x".getBytes());
        scan.addFamily(toBytes(family1));
        scan.setMaxVersions(10);
        ResultScanner scanner = multiCfHTable.getScanner(scan);
        int key1Cnt = 0, key2Cnt = 0, key3Cnt = 0;
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                if (key1.equals(Bytes.toString(CellUtil.cloneRow(cell)))) {
                    ++key1Cnt;
                } else if (key2.equals(Bytes.toString(CellUtil.cloneRow(cell)))) {
                    ++key2Cnt;
                } else {
                    ++key3Cnt;
                }
            }
        }
        Assert.assertEquals(1, key1Cnt);
        Assert.assertEquals(1, key2Cnt);
        Assert.assertEquals(1, key3Cnt);

        multiCfHTable.delete(deleteKey1Family);
        multiCfHTable.delete(deleteKey2Family);
        multiCfHTable.delete(deleteKey3Family);

        // test DeleteFamilyVersion multiple cf
        Put putKey1Fam1Column3Ts4 = new Put(toBytes(key1));
        putKey1Fam1Column3Ts4.addColumn(toBytes(family1), toBytes(column3), timeStamp4,
            toBytes(value3));

        Put putKey1Fam2Column2Ts2 = new Put(toBytes(key1));
        putKey1Fam2Column2Ts2.addColumn(toBytes(family2), toBytes(column2), timeStamp2,
            toBytes(value1));

        Put putKey1Fam2Column3Ts2 = new Put(toBytes(key1));
        putKey1Fam2Column3Ts2.addColumn(toBytes(family2), toBytes(column3), timeStamp2,
            toBytes(value1));

        Put putKey1Fam1Column2Ts1 = new Put(toBytes(key1));
        putKey1Fam1Column2Ts1.addColumn(toBytes(family1), toBytes(column2), timeStamp1,
            toBytes(value2));

        Put putKey2Fam1Column2Ts5 = new Put(toBytes(key2));
        putKey2Fam1Column2Ts5.addColumn(toBytes(family1), toBytes(column2), timeStamp5,
            toBytes(value2));

        Put putKey2Fam2Column3Ts1 = new Put(toBytes(key2));
        putKey2Fam2Column3Ts1.addColumn(toBytes(family2), toBytes(column3), timeStamp3,
            toBytes(value3));

        Put putKey2Fam1Column1Ts5 = new Put(toBytes(key2));
        putKey2Fam1Column1Ts5.addColumn(toBytes(family1), toBytes(column1), timeStamp5,
            toBytes(value1));

        Put putKey2Fam2Column1Ts3 = new Put(toBytes(key2));
        putKey2Fam2Column1Ts3.addColumn(toBytes(family2), toBytes(column1), timeStamp3,
            toBytes(value2));

        Put putKey3Fam1Column2Ts6 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts6.addColumn(toBytes(family1), toBytes(column2), timeStamp6,
            toBytes(value2));

        Put putKey3Fam2Column3Ts7 = new Put(toBytes(key3));
        putKey3Fam2Column3Ts7.addColumn(toBytes(family2), toBytes(column3), timeStamp7,
            toBytes(value1));

        Put putKey3Fam2Column1Ts7 = new Put(toBytes(key3));
        putKey3Fam2Column1Ts7.addColumn(toBytes(family2), toBytes(column1), timeStamp7,
            toBytes(value2));

        Put putKey3Fam1Column2Ts2 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts2.addColumn(toBytes(family1), toBytes(column2), timeStamp2,
            toBytes(value1));

        tryPut(multiCfHTable, putKey1Fam1Column3Ts4);
        tryPut(multiCfHTable, putKey1Fam2Column2Ts2);
        tryPut(multiCfHTable, putKey1Fam2Column3Ts2);
        tryPut(multiCfHTable, putKey1Fam1Column2Ts1);
        tryPut(multiCfHTable, putKey2Fam1Column2Ts5);
        tryPut(multiCfHTable, putKey2Fam2Column3Ts1);
        tryPut(multiCfHTable, putKey2Fam1Column1Ts5);
        tryPut(multiCfHTable, putKey2Fam2Column1Ts3);
        tryPut(multiCfHTable, putKey3Fam1Column2Ts6);
        tryPut(multiCfHTable, putKey3Fam2Column3Ts7);
        tryPut(multiCfHTable, putKey3Fam2Column1Ts7);
        tryPut(multiCfHTable, putKey3Fam1Column2Ts2);

        Get getKey1 = new Get(toBytes(key1));
        getKey1.addFamily(toBytes(family1));
        getKey1.addFamily(toBytes(family2));
        getKey1.setMaxVersions(10);
        r = multiCfHTable.get(getKey1);
        Assert.assertEquals(4, r.getRow().length);

        Get getKey2 = new Get(toBytes(key2));
        getKey2.addFamily(toBytes(family1));
        getKey2.addFamily(toBytes(family2));
        getKey2.setMaxVersions(10);
        r = multiCfHTable.get(getKey2);
        Assert.assertEquals(4, r.getRow().length);

        Get getKey3 = new Get(toBytes(key3));
        getKey3.addFamily(toBytes(family1));
        getKey3.addFamily(toBytes(family2));
        getKey3.setMaxVersions(10);
        r = multiCfHTable.get(getKey3);
        Assert.assertEquals(4, r.getRow().length);

        Delete delKey1Ts_6_2 = new Delete(toBytes(key1));
        delKey1Ts_6_2.addFamilyVersion(toBytes(family1), timeStamp4);
        delKey1Ts_6_2.addFamilyVersion(toBytes(family2), timeStamp2);
        multiCfHTable.delete(delKey1Ts_6_2);

        getKey1 = new Get(toBytes(key1));
        getKey1.addFamily(toBytes(family1));
        getKey1.addFamily(toBytes(family2));
        getKey1.setMaxVersions(10);
        r = multiCfHTable.get(getKey1);
        Assert.assertEquals(1, r.getRow().length);
        for (Cell cell : r.rawCells()) {
            Assert.assertEquals(timeStamp1, cell.getTimestamp());
        }

        Delete delKey2Ts_5_3 = new Delete(toBytes(key2));
        delKey2Ts_5_3.addFamilyVersion(toBytes(family1), timeStamp5);
        delKey2Ts_5_3.addFamilyVersion(toBytes(family2), timeStamp3);
        multiCfHTable.delete(delKey2Ts_5_3);

        getKey2 = new Get(toBytes(key2));
        getKey2.addFamily(toBytes(family1));
        getKey2.addFamily(toBytes(family2));
        getKey2.setMaxVersions(10);
        r = multiCfHTable.get(getKey2);
        Assert.assertEquals(0, r.getRow().length);

        Delete delKey3Ts_2_7 = new Delete(toBytes(key3));
        delKey3Ts_2_7.addFamilyVersion(toBytes(family1), timeStamp2);
        delKey3Ts_2_7.addFamilyVersion(toBytes(family2), timeStamp7);
        multiCfHTable.delete(delKey3Ts_2_7);

        getKey3 = new Get(toBytes(key3));
        getKey3.addFamily(toBytes(family1));
        getKey3.addFamily(toBytes(family2));
        getKey3.setMaxVersions(10);
        r = multiCfHTable.get(getKey3);
        Assert.assertEquals(1, r.getRow().length);
        for (Cell cell : r.rawCells()) {
            Assert.assertEquals(timeStamp6, cell.getTimestamp());
        }

        scan = new Scan();
        scan.setStartRow(toBytes(key1));
        scan.setStopRow("scanKey4x".getBytes());
        scan.addFamily(toBytes(family1));
        scan.addFamily(toBytes(family2));
        scan.setMaxVersions(10);
        scanner = multiCfHTable.getScanner(scan);
        int ts1Cnt = 0, ts9Cnt = 0;
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                if (cell.getTimestamp() == timeStamp1) {
                    ++ts1Cnt;
                } else if (cell.getTimestamp() == timeStamp6) {
                    ++ts9Cnt;
                }
            }
        }
        Assert.assertEquals(1, ts1Cnt);
        Assert.assertEquals(1, ts9Cnt);

        multiCfHTable.delete(deleteKey1Family);
        multiCfHTable.delete(deleteKey2Family);
        multiCfHTable.delete(deleteKey3Family);
    }

    @Test
    public void testMultiColumnFamilyTableBuilder() throws Exception {
        byte[] family1 = "family_with_group1".getBytes();
        byte[] family2 = "family_with_group2".getBytes();
        byte[] family3 = "family_with_group3".getBytes();

        byte[] family1_column1 = "family1_column1".getBytes();
        byte[] family1_column2 = "family1_column2".getBytes();
        byte[] family1_column3 = "family1_column3".getBytes();
        byte[] family2_column1 = "family2_column1".getBytes();
        byte[] family2_column2 = "family2_column2".getBytes();
        byte[] family3_column1 = "family3_column1".getBytes();
        byte[] family1_value = "VVV1".getBytes();
        byte[] family2_value = "VVV2".getBytes();
        byte[] family3_value = "VVV3".getBytes();

        Configuration conf = ObHTableTestUtil.newConfiguration();
        conf.set("rs.list.acquire.read.timeout", "10000");
        conf.set(SOCKET_TIMEOUT_CONNECT, "15000");
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("test_multi_cf");
        TableBuilder builder = connection.getTableBuilder(tableName, null);
        // build a OHTable with default params
        multiCfHTable = builder.build();

        Delete delete = new Delete(toBytes("Key0"));
        delete.addFamily(family1);
        delete.addFamily(family2);
        delete.addFamily(family3);
        multiCfHTable.delete(delete);

        Put put = new Put(toBytes("Key0"));
        put.addColumn(family1, family1_column1, family1_value);
        put.addColumn(family1, family1_column2, family1_value);
        put.addColumn(family1, family1_column3, family1_value);
        put.addColumn(family2, family2_column1, family2_value);
        put.addColumn(family2, family2_column2, family2_value);
        put.addColumn(family3, family3_column1, family3_value);
        multiCfHTable.put(put);

        int count = 0;
        Get get = new Get(toBytes("Key0"));
        get.setMaxVersions();
        Result r = multiCfHTable.get(get);
        Assert.assertEquals(6, r.rawCells().length);

        delete = new Delete(toBytes("Key0"));
        delete.addFamily(family1);
        delete.addFamily(family2);
        delete.addFamily(family3);
        multiCfHTable.delete(delete);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        // set params for TableBuilder
        builder.setOperationTimeout(1500000);
        builder.setRpcTimeout(40000);
        multiCfHTable = builder.build();

        put = new Put(toBytes("Key0"));
        put.addColumn(family1, family1_column1, family1_value);
        put.addColumn(family1, family1_column2, family1_value);
        put.addColumn(family2, family2_column1, family2_value);
        put.addColumn(family3, family3_column1, family3_value);
        multiCfHTable.put(put);

        r = multiCfHTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        multiCfHTable.delete(delete);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);
    }

    @Test
    public void testMultiColumnFamilyBufferedMutator() throws Exception {
        byte[] family1 = "family_with_group1".getBytes();
        byte[] family2 = "family_with_group2".getBytes();
        byte[] family3 = "family_with_group3".getBytes();

        byte[] family1_column1 = "family1_column1".getBytes();
        byte[] family1_column2 = "family1_column2".getBytes();
        byte[] family1_column3 = "family1_column3".getBytes();
        byte[] family2_column1 = "family2_column1".getBytes();
        byte[] family2_column2 = "family2_column2".getBytes();
        byte[] family3_column1 = "family3_column1".getBytes();
        byte[] family3_column2 = "family3_column2".getBytes();
        byte[] family1_value = "VVV1".getBytes();
        byte[] family2_value = "VVV2".getBytes();
        byte[] family3_value = "VVV3".getBytes();

        Configuration conf = ObHTableTestUtil.newConfiguration();
        TableName tableName = TableName.valueOf("test_multi_cf");
        Connection connection = ConnectionFactory.createConnection(conf);
        BufferedMutator mutator = connection.getBufferedMutator(tableName);

        int rows = 10;
        List<String> keys = new ArrayList<>();
        List<Mutation> mutations = new ArrayList<>();
        for (int i = 0; i < rows; ++i) {
            String key = "Key" + i;
            keys.add(key);
            Delete delete = new Delete(toBytes(key));
            mutations.add(delete);
            Put put = new Put(toBytes(key));
            put.addColumn(family1, family1_column1, family1_value);
            put.addColumn(family1, family1_column2, family1_value);
            put.addColumn(family1, family1_column3, family1_value);
            put.addColumn(family2, family2_column1, family2_value);
            put.addColumn(family2, family2_column2, family2_value);
            put.addColumn(family3, family3_column1, family3_value);
            mutations.add(put);
        }
        mutator.mutate(mutations);

        // test force flush
        mutator.flush();
        Get get = new Get(toBytes("Key2"));
        get.addFamily(family1);
        get.addFamily(family2);
        Result result = multiCfHTable.get(get);
        Assert.assertEquals(5, result.rawCells().length);

        mutations.clear();
        for (int i = 0; i < rows; ++i) {
            if (i % 5 == 0) { // 0, 5
                Delete delete = new Delete(toBytes("Key" + i));
                delete.addFamily(family2);
                delete.addFamily(family3);
                mutations.add(delete);
            }
        }
        mutator.mutate(mutations);
        mutator.flush();

        get = new Get(toBytes("Key0"));
        result = multiCfHTable.get(get);
        Assert.assertEquals(3, result.rawCells().length);
        Assert.assertFalse(result.containsColumn(family2, family2_column1));
        Assert.assertFalse(result.containsColumn(family2, family2_column2));
        Assert.assertFalse(result.containsColumn(family3, family3_column1));

        get = new Get(toBytes("Key5"));
        result = multiCfHTable.get(get);
        Assert.assertEquals(3, result.rawCells().length);
        Assert.assertFalse(result.containsColumn(family2, family2_column1));
        Assert.assertFalse(result.containsColumn(family2, family2_column2));
        Assert.assertFalse(result.containsColumn(family3, family3_column1));

        mutations.clear();
        for (String key : keys) {
            Delete delete = new Delete(toBytes(key));
            mutations.add(delete);
        }
        mutator.mutate(mutations);
        mutator.flush();

        Scan scan = new Scan();
        scan.setStartRow(toBytes("Key0"));
        scan.setStopRow(toBytes("Key10"));
        scan.addFamily(family1);
        scan.addFamily(family2);
        scan.addFamily(family3);
        ResultScanner scanner = multiCfHTable.getScanner(scan);
        int count = 0;
        for (Result r : scanner) {
            count += r.rawCells().length;
        }
        Assert.assertEquals(0, count);

        // test auto flush
        long bufferSize = 45000L;
        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        params.writeBufferSize(bufferSize);
        mutator = connection.getBufferedMutator(params);

        while (true) {
            for (int i = 0; i < rows; ++i) {
                mutations.clear();
                Put put = new Put(toBytes(keys.get(i)));
                put.addColumn(family1, family1_column1, family1_value);
                put.addColumn(family1, family1_column2, family1_value);
                put.addColumn(family1, family1_column3, family1_value);
                put.addColumn(family2, family2_column1, family2_value);
                put.addColumn(family3, family3_column1, family2_value);
                put.addColumn(family3, family3_column2, family3_value);
                mutations.add(put);
                if (i % 3 == 0) { // 0, 3, 6, 9
                    Delete delete = new Delete(toBytes(keys.get(i)));
                    delete.addFamily(family1);
                    delete.addFamily(family2);
                    mutations.add(delete);
                }
                mutator.mutate(mutations);
            }

            get = new Get(toBytes("Key0"));
            result = multiCfHTable.get(get);
            if (!result.isEmpty()) {
                break;
            }
        }
        get = new Get(toBytes("Key2"));
        result = multiCfHTable.get(get);
        Assert.assertEquals(6 , result.rawCells().length);
        Assert.assertTrue(result.containsColumn(family1, family1_column1));
        Assert.assertTrue(result.containsColumn(family1, family1_column2));
        Assert.assertTrue(result.containsColumn(family1, family1_column3));
        Assert.assertTrue(result.containsColumn(family2, family2_column1));
        Assert.assertTrue(result.containsColumn(family3, family3_column1));
        Assert.assertTrue(result.containsColumn(family3, family3_column2));

        get = new Get(toBytes("Key3"));
        result = multiCfHTable.get(get);
        if (result.containsColumn(family1, family1_column1) || result.containsColumn(family2, family2_column1)) {
            mutator.flush();
        }
        get = new Get(toBytes("Key3"));
        result = multiCfHTable.get(get);
        Assert.assertEquals(2, result.rawCells().length);
        Assert.assertFalse(result.containsColumn(family1, family1_column1));
        Assert.assertFalse(result.containsColumn(family1, family1_column2));
        Assert.assertFalse(result.containsColumn(family1, family1_column3));
        Assert.assertFalse(result.containsColumn(family2, family2_column1));
        Assert.assertTrue(result.containsColumn(family3, family3_column1));
        Assert.assertTrue(result.containsColumn(family3, family3_column2));

        get = new Get(toBytes("Key9"));
        result = multiCfHTable.get(get);
        if (result.containsColumn(family1, family1_column1) || result.containsColumn(family2, family2_column1)) {
            mutator.flush();
        }
        get = new Get(toBytes("Key9"));
        result = multiCfHTable.get(get);
        Assert.assertEquals(2, result.rawCells().length);
        Assert.assertFalse(result.containsColumn(family1, family1_column1));
        Assert.assertFalse(result.containsColumn(family1, family1_column2));
        Assert.assertFalse(result.containsColumn(family1, family1_column3));
        Assert.assertFalse(result.containsColumn(family2, family2_column1));
        Assert.assertTrue(result.containsColumn(family3, family3_column1));
        Assert.assertTrue(result.containsColumn(family3, family3_column2));

        mutations.clear();
        for (String key : keys) {
            Delete delete = new Delete(toBytes(key));
            mutations.add(delete);
        }
        mutator.mutate(mutations);
        mutator.flush();

        scan = new Scan();
        scan.setStartRow(toBytes("Key0"));
        scan.setStopRow(toBytes("Key10"));
        scan.addFamily(family1);
        scan.addFamily(family2);
        scan.addFamily(family3);
        scanner = multiCfHTable.getScanner(scan);
        count = 0;
        for (Result r : scanner) {
            count += r.rawCells().length;
        }
        Assert.assertEquals(0, count);

        // test periodic flush
        params.setWriteBufferPeriodicFlushTimeoutMs(100);
        mutator = connection.getBufferedMutator(params);
        while (true) {
            for (int i = 0; i < rows; ++i) {
                mutations.clear();
                Put put = new Put(toBytes(keys.get(i)));
                put.addColumn(family1, family1_column1, family1_value);
                put.addColumn(family1, family1_column2, family1_value);
                put.addColumn(family1, family1_column3, family1_value);
                put.addColumn(family2, family2_column1, family2_value);
                put.addColumn(family3, family3_column1, family2_value);
                put.addColumn(family3, family3_column2, family3_value);
                mutations.add(put);
                if (i % 3 == 0) { // 0, 3, 6, 9
                    Delete delete = new Delete(toBytes(keys.get(i)));
                    delete.addFamily(family1);
                    delete.addFamily(family2);
                    mutations.add(delete);
                }
                mutator.mutate(mutations);
            }

            get = new Get(toBytes("Key0"));
            result = multiCfHTable.get(get);
            if (!result.isEmpty()) {
                break;
            }
        }
        get = new Get(toBytes("Key2"));
        get.setMaxVersions();
        result = multiCfHTable.get(get);
        count = result.rawCells().length;
        Assert.assertTrue(count > 0);
        // test timer periodic flush
        int lastUndealtCount = ((OHBufferedMutatorImpl) mutator).size();
        Thread.sleep(1000);
        int currentUndealtCount = ((OHBufferedMutatorImpl) mutator).size();
        Assert.assertNotEquals(lastUndealtCount, currentUndealtCount);
        // after periodic flush, all mutations will be committed
        Assert.assertEquals(0, currentUndealtCount);
        result = multiCfHTable.get(get);
        int newCount = result.rawCells().length;
        Assert.assertNotEquals(count, newCount);

        // clean data
        mutations.clear();
        for (String key : keys) {
            Delete delete = new Delete(toBytes(key));
            mutations.add(delete);
        }
        mutator.mutate(mutations);
        mutator.flush();
        mutator.close();

        scan = new Scan();
        scan.setStartRow(toBytes("Key0"));
        scan.setStopRow(toBytes("Key10"));
        scan.addFamily(family1);
        scan.addFamily(family2);
        scan.addFamily(family3);
        scanner = multiCfHTable.getScanner(scan);
        count = 0;
        for (Result r : scanner) {
            count += r.rawCells().length;
        }
        Assert.assertEquals(0, count);
    }

    @Test
    public void testMultiColumnFamilyBatch() throws Exception {
        byte[] family1 = "family_with_group1".getBytes();
        byte[] family2 = "family_with_group2".getBytes();
        byte[] family3 = "family_with_group3".getBytes();

        byte[] family1_column1 = "family1_column1".getBytes();
        byte[] family1_column2 = "family1_column2".getBytes();
        byte[] family1_column3 = "family1_column3".getBytes();
        byte[] family2_column1 = "family2_column1".getBytes();
        byte[] family2_column2 = "family2_column2".getBytes();
        byte[] family3_column1 = "family3_column1".getBytes();
        byte[] family1_value = "VVV1".getBytes();
        byte[] family2_value = "VVV2".getBytes();
        byte[] family3_value = "VVV3".getBytes();

        int rows = 10;
        List<Row> batchLsit = new LinkedList<>();
        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            Delete delete = new Delete(toBytes("Key" + i));
            batchLsit.add(delete);
            put.addColumn(family1, family1_column1, family1_value);
            put.addColumn(family1, family1_column2, family1_value);
            put.addColumn(family1, family1_column3, family1_value);
            put.addColumn(family2, family2_column1, family2_value);
            put.addColumn(family2, family2_column2, family2_value);
            put.addColumn(family3, family3_column1, family3_value);
            batchLsit.add(put);
        }

        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        Delete delete = new Delete(toBytes("Key1"));
        delete.addColumns(family1, family1_column1);
        delete.addColumns(family2, family2_column1);
        batchLsit.add(delete);
        Object[] results = new Object[batchLsit.size()];
        multiCfHTable.batch(batchLsit, results);
        // f1c2 f1c3 f2c2 f3c1
        Get get = new Get(toBytes("Key1"));
        Result result = multiCfHTable.get(get);
        Cell[] keyValues = result.rawCells();
        assertEquals(4, keyValues.length);
        assertFalse(result.containsColumn(family1, family1_column1));
        assertFalse(result.containsColumn(family2, family2_column1));

        assertTrue(result.containsColumn(family1, family1_column2));
        assertArrayEquals(result.getValue(family1, family1_column2), family1_value);
        assertTrue(result.containsColumn(family1, family1_column3));
        assertArrayEquals(result.getValue(family1, family1_column3), family1_value);
        assertTrue(result.containsColumn(family2, family2_column2));
        assertArrayEquals(result.getValue(family2, family2_column2), family2_value);
        assertTrue(result.containsColumn(family3, family3_column1));
        assertArrayEquals(result.getValue(family3, family3_column1), family3_value);

        // f1c1 f2c1 f2c2
        delete = new Delete(toBytes("Key2"));
        delete.addColumns(family1, family1_column2);
        delete.addColumns(family1, family1_column3);
        delete.addColumns(family3, family3_column1);
        batchLsit.add(delete);
        // null
        results = new Object[batchLsit.size()];
        multiCfHTable.batch(batchLsit, results);
        get = new Get(toBytes("Key2"));
        result = multiCfHTable.get(get);
        keyValues = result.rawCells();
        assertEquals(3, keyValues.length);
        batchLsit.clear();
        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            put.addColumn(family1, family1_column1, family1_value);
            put.addColumn(family1, family1_column2, family1_value);
            put.addColumn(family1, family1_column3, family1_value);
            put.addColumn(family2, family2_column1, family2_value);
            put.addColumn(family2, family2_column2, family2_value);
            put.addColumn(family3, family3_column1, family3_value);
            batchLsit.add(put);
        }

        delete = new Delete(toBytes("Key3"));
        delete.addColumn(family1, family1_column2);
        delete.addColumn(family2, family2_column1);
        batchLsit.add(delete);
        results = new Object[batchLsit.size()];
        multiCfHTable.batch(batchLsit, results);
        get = new Get(toBytes("Key3"));
        result = multiCfHTable.get(get);
        keyValues = result.rawCells();
        assertEquals(6, keyValues.length);

        batchLsit.clear();
        delete = new Delete(toBytes("Key4"));
        delete.addColumns(family1, family1_column2);
        delete.addColumns(family2, family2_column1);
        delete.addFamily(family3);
        batchLsit.add(delete);
        results = new Object[batchLsit.size()];
        multiCfHTable.batch(batchLsit, results);
        get = new Get(toBytes("Key4"));
        get.setMaxVersions(10);
        result = multiCfHTable.get(get);
        keyValues = result.rawCells();
        assertEquals(6, keyValues.length);

        batchLsit.clear();
        final long[] updateCounter = new long[] { 0L };
        delete = new Delete(toBytes("Key5"));
        delete.addColumns(family1, family1_column2);
        delete.addColumns(family2, family2_column1);
        delete.addFamily(family3);
        batchLsit.add(delete);
        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            put.addColumn(family1, family1_column1, family1_value);
            put.addColumn(family1, family1_column2, family1_value);
            put.addColumn(family1, family1_column3, family1_value);
            put.addColumn(family2, family2_column1, family2_value);
            put.addColumn(family2, family2_column2, family2_value);
            put.addColumn(family3, family3_column1, family3_value);
            batchLsit.add(put);
        }
        results = new Object[batchLsit.size()];
        multiCfHTable.batchCallback(batchLsit, results, new Batch.Callback<MutationResult>() {
            @Override
            public void update(byte[] region, byte[] row, MutationResult result) {
                updateCounter[0]++;
            }
        });
        assertEquals(11, updateCounter[0]);

    }

    @Test
    public void testMultiColumnFamilyPut() throws Exception {
        byte[] family1 = "family_with_group1".getBytes();
        byte[] family2 = "family_with_group2".getBytes();
        byte[] family3 = "family_with_group3".getBytes();

        byte[] family1_column1 = "family1_column1".getBytes();
        byte[] family1_column2 = "family1_column2".getBytes();
        byte[] family1_column3 = "family1_column3".getBytes();
        byte[] family2_column1 = "family2_column1".getBytes();
        byte[] family2_column2 = "family2_column2".getBytes();
        byte[] family3_column1 = "family3_column1".getBytes();
        byte[] family1_value = "VVV1".getBytes();
        byte[] family2_value = "VVV2".getBytes();
        byte[] family3_value = "VVV3".getBytes();

        Map<byte[], byte[]> expectedValues = new HashMap<>();
        expectedValues.put(family1_column1, family1_value);
        expectedValues.put(family1_column2, family1_value);
        expectedValues.put(family1_column3, family1_value);
        expectedValues.put(family2_column1, family2_value);
        expectedValues.put(family2_column2, family2_value);
        expectedValues.put(family3_column1, family3_value);

        int rows = 30;

        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            put.addColumn(family1, family1_column1, family1_value);
            put.addColumn(family1, family1_column2, family1_value);
            put.addColumn(family1, family1_column3, family1_value);
            put.addColumn(family2, family2_column1, family2_value);
            put.addColumn(family2, family2_column2, family2_value);
            put.addColumn(family3, family3_column1, family3_value);
            multiCfHTable.put(put);
        }

        Scan scan = new Scan();
        scan.setStartRow(toBytes("Key"));
        scan.setStopRow(toBytes("Kf"));
        ResultScanner scanner = multiCfHTable.getScanner(scan);
        int count = 0;

        for (Result result : scanner) {
            Cell[] keyValues = result.rawCells();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = CellUtil.cloneQualifier(keyValues[i]);
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, CellUtil.cloneValue(keyValues[i]));
                }
            }
            count++;
        }
        assertEquals(count, rows);
    }

    @Ignore
    public void testMultiColumnFamilyAppend() throws Exception {
        byte[] family1 = "family_with_group1".getBytes();
        byte[] family2 = "family_with_group2".getBytes();
        byte[] family3 = "family_with_group3".getBytes();

        byte[] family1_column1 = "family1_column1".getBytes();
        byte[] family1_column2 = "family1_column2".getBytes();
        byte[] family1_column3 = "family1_column3".getBytes();
        byte[] family2_column1 = "family2_column1".getBytes();
        byte[] family2_column2 = "family2_column2".getBytes();
        byte[] family3_column1 = "family3_column1".getBytes();
        byte[] family1_value = "VVV1".getBytes();
        byte[] family2_value = "VVV2".getBytes();
        byte[] family3_value = "VVV3".getBytes();

        Map<byte[], byte[]> expectedValues = new HashMap<>();
        expectedValues.put(family1_column1, family1_value);
        expectedValues.put(family1_column2, family1_value);
        expectedValues.put(family1_column3, family1_value);
        expectedValues.put(family2_column1, family2_value);
        expectedValues.put(family2_column2, family2_value);
        expectedValues.put(family3_column1, family3_value);

        int rows = 30;

        for (int i = 0; i < rows; ++i) {
            Append append = new Append(toBytes("Key" + i));
            append.add(family1, family1_column1, family1_value);
            append.add(family1, family1_column2, family1_value);
            append.add(family1, family1_column3, family1_value);
            append.add(family2, family2_column1, family2_value);
            append.add(family2, family2_column2, family2_value);
            append.add(family3, family3_column1, family3_value);
            multiCfHTable.append(append);
        }

        Scan scan = new Scan();
        scan.setStartRow(toBytes("Key"));
        scan.setStopRow(toBytes("Kf"));
        ResultScanner scanner = multiCfHTable.getScanner(scan);
        int count = 0;

        for (Result result : scanner) {
            Cell[] keyValues = result.rawCells();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = CellUtil.cloneQualifier(keyValues[i]);
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, CellUtil.cloneValue(keyValues[i]));
                }
            }
            count++;
        }
        assertEquals(count, rows);
    }

    @Test
    public void testMultiColumnFamilyReverseScan() throws Exception {
        byte[] family1 = "family_with_group1".getBytes();
        byte[] family2 = "family_with_group2".getBytes();
        byte[] family3 = "family_with_group3".getBytes();

        byte[] family1_column1 = "family1_column1".getBytes();
        byte[] family1_column2 = "family1_column2".getBytes();
        byte[] family1_column3 = "family1_column3".getBytes();
        byte[] family2_column1 = "family2_column1".getBytes();
        byte[] family2_column2 = "family2_column2".getBytes();
        byte[] family3_column1 = "family3_column1".getBytes();
        byte[] family1_value = "VVV1".getBytes();
        byte[] family2_value = "VVV2".getBytes();
        byte[] family3_value = "VVV3".getBytes();

        Map<byte[], byte[]> expectedValues = new HashMap<>();
        expectedValues.put(family1_column1, family1_value);
        expectedValues.put(family1_column2, family1_value);
        expectedValues.put(family1_column3, family1_value);
        expectedValues.put(family2_column1, family2_value);
        expectedValues.put(family2_column2, family2_value);
        expectedValues.put(family3_column1, family3_value);

        int rows = 30;

        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            put.addColumn(family1, family1_column1, family1_value);
            put.addColumn(family1, family1_column2, family1_value);
            put.addColumn(family1, family1_column3, family1_value);
            put.addColumn(family2, family2_column1, family2_value);
            put.addColumn(family2, family2_column2, family2_value);
            put.addColumn(family3, family3_column1, family3_value);
            multiCfHTable.put(put);
        }

        Scan scan = new Scan();
        scan.addFamily(family1);
        scan.addFamily(family2);
        scan.setReversed(true);
        ResultScanner scanner2 = multiCfHTable.getScanner(scan);

        for (Result result : scanner2) {
            Cell[] keyValues = result.rawCells();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = CellUtil.cloneQualifier(keyValues[i]);
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, CellUtil.cloneValue(keyValues[i]));
                }
            }
        }
    }

    @Test
    public void testMultiColumnFamilyScanWithColumns() throws Exception {
        byte[] family1 = "family_with_group1".getBytes();
        byte[] family2 = "family_with_group2".getBytes();
        byte[] family3 = "family_with_group3".getBytes();

        byte[] family1_column1 = "family1_column1".getBytes();
        byte[] family1_column2 = "family1_column2".getBytes();
        byte[] family1_column3 = "family1_column3".getBytes();
        byte[] family2_column1 = "family2_column1".getBytes();
        byte[] family2_column2 = "family2_column2".getBytes();
        byte[] family3_column1 = "family3_column1".getBytes();
        byte[] family1_value = "VVV1".getBytes();
        byte[] family2_value = "VVV2".getBytes();
        byte[] family3_value = "VVV3".getBytes();

        Map<byte[], byte[]> expectedValues = new HashMap<>();
        expectedValues.put(family1_column1, family1_value);
        expectedValues.put(family1_column2, family1_value);
        expectedValues.put(family1_column3, family1_value);
        expectedValues.put(family2_column1, family2_value);
        expectedValues.put(family2_column2, family2_value);
        expectedValues.put(family3_column1, family3_value);

        int rows = 30;

        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            put.addColumn(family1, family1_column1, family1_value);
            put.addColumn(family1, family1_column2, family1_value);
            put.addColumn(family1, family1_column3, family1_value);
            put.addColumn(family2, family2_column1, family2_value);
            put.addColumn(family2, family2_column2, family2_value);
            put.addColumn(family3, family3_column1, family3_value);
            multiCfHTable.put(put);
        }

        Scan scan = new Scan();
        scan.setStartRow(toBytes("Key"));
        scan.setStopRow(toBytes("Kf"));
        scan.addColumn(family1, family1_column1);
        scan.addColumn(family2, family2_column1);
        ResultScanner scanner = multiCfHTable.getScanner(scan);

        for (Result result : scanner) {
            Cell[] keyValues = result.rawCells();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = CellUtil.cloneQualifier(keyValues[i]);
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, CellUtil.cloneValue(keyValues[i]));
                }
            }
            assertEquals(2, keyValues.length);
        }
        scanner.close();
        scan = new Scan();
        scan.setStartRow(toBytes("Key"));
        scan.setStopRow(toBytes("Kf"));
        scan.addColumn(family1, family1_column1);
        scan.addColumn(family1, family1_column2);
        scan.addColumn(family1, family1_column3);
        scan.addColumn(family2, family2_column1);
        scan.addColumn(family2, family2_column2);
        scanner = multiCfHTable.getScanner(scan);

        for (Result result : scanner) {
            Cell[] keyValues = result.rawCells();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = CellUtil.cloneQualifier(keyValues[i]);
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, CellUtil.cloneValue(keyValues[i]));
                }
            }
            assertEquals(5, keyValues.length);
        }
        scanner.close();
        scan = new Scan();
        scan.setStartRow(toBytes("Key"));
        scan.setStopRow(toBytes("Kf"));
        scan.addFamily(family1);
        scan.addFamily(family2);

        scanner = multiCfHTable.getScanner(scan);

        for (Result result : scanner) {
            Cell[] keyValues = result.rawCells();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = CellUtil.cloneQualifier(keyValues[i]);
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, CellUtil.cloneValue(keyValues[i]));
                }
            }
            assertEquals(5, keyValues.length);
        }
        scanner.close();
        scan = new Scan();
        scan.setStartRow(toBytes("Key"));
        scan.setStopRow(toBytes("Kf"));
        scan.addFamily(family1);
        scan.addFamily(family3);

        scanner = multiCfHTable.getScanner(scan);

        for (Result result : scanner) {
            Cell[] keyValues = result.rawCells();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = CellUtil.cloneQualifier(keyValues[i]);
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, CellUtil.cloneValue(keyValues[i]));
                }
            }
            // f1c1 f1c2 f1c3 f3c1
            assertEquals(4, keyValues.length);
        }
        scanner.close();
    }

    @Test
    public void testMultiColumnFamilyScanWithFilter() throws Exception {
        byte[] family1 = "family_with_group1".getBytes();
        byte[] family2 = "family_with_group2".getBytes();
        byte[] family3 = "family_with_group3".getBytes();

        byte[] family1_column1 = "family1_column1".getBytes();
        byte[] family1_column2 = "family1_column2".getBytes();
        byte[] family1_column3 = "family1_column3".getBytes();
        byte[] family2_column1 = "family2_column1".getBytes();
        byte[] family2_column2 = "family2_column2".getBytes();
        byte[] family3_column1 = "family3_column1".getBytes();
        byte[] family1_value = "VVV1".getBytes();
        byte[] family2_value = "VVV2".getBytes();
        byte[] family3_value = "VVV3".getBytes();

        Map<byte[], byte[]> expectedValues = new HashMap<>();
        expectedValues.put(family1_column1, family1_value);
        expectedValues.put(family1_column2, family1_value);
        expectedValues.put(family1_column3, family1_value);
        expectedValues.put(family2_column1, family2_value);
        expectedValues.put(family2_column2, family2_value);
        expectedValues.put(family3_column1, family3_value);

        int rows = 30;

        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            put.addColumn(family1, family1_column1, family1_value);
            put.addColumn(family1, family1_column2, family1_value);
            put.addColumn(family1, family1_column3, family1_value);
            put.addColumn(family2, family2_column1, family2_value);
            put.addColumn(family2, family2_column2, family2_value);
            put.addColumn(family3, family3_column1, family3_value);
            multiCfHTable.put(put);
        }

        PrefixFilter filter = new PrefixFilter(toBytes("Key1"));
        Scan scan = new Scan();
        scan.setStartRow(toBytes("Key"));
        scan.setStopRow(toBytes("Kf"));
        scan.setFilter(filter);
        ResultScanner scanner = multiCfHTable.getScanner(scan);

        // Key1, Key10, Key11, Key12, Key13, Key14, Key15, Key16, Key17, Key18, Key19
        int count = 0;
        for (Result result : scanner) {
            Cell[] keyValues = result.rawCells();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = CellUtil.cloneQualifier(keyValues[i]);
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, CellUtil.cloneValue(keyValues[i]));
                }
            }
            assertEquals(6, keyValues.length);
            count++;
        }
        assertEquals(11, count);
    }

    @Test
    public void testMultiColumnFamilyGet() throws Exception {
        byte[] family1 = "family_with_group1".getBytes();
        byte[] family2 = "family_with_group2".getBytes();
        byte[] family3 = "family_with_group3".getBytes();

        byte[] family1_column1 = "family1_column1".getBytes();
        byte[] family1_column2 = "family1_column2".getBytes();
        byte[] family1_column3 = "family1_column3".getBytes();
        byte[] family2_column1 = "family2_column1".getBytes();
        byte[] family2_column2 = "family2_column2".getBytes();
        byte[] family3_column1 = "family3_column1".getBytes();
        byte[] family1_value = "VVV1".getBytes();
        byte[] family2_value = "VVV2".getBytes();
        byte[] family3_value = "VVV3".getBytes();

        Map<byte[], byte[]> expectedValues = new HashMap<>();
        expectedValues.put(family1_column1, family1_value);
        expectedValues.put(family1_column2, family1_value);
        expectedValues.put(family1_column3, family1_value);
        expectedValues.put(family2_column1, family2_value);
        expectedValues.put(family2_column2, family2_value);
        expectedValues.put(family3_column1, family3_value);

        int rows = 3;

        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            put.addColumn(family1, family1_column1, family1_value);
            put.addColumn(family1, family1_column2, family1_value);
            put.addColumn(family1, family1_column3, family1_value);
            put.addColumn(family2, family2_column1, family2_value);
            put.addColumn(family2, family2_column2, family2_value);
            put.addColumn(family3, family3_column1, family3_value);
            multiCfHTable.put(put);
        }

        // get with empty family
        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        Get get = new Get(toBytes("Key1"));
        Result result = multiCfHTable.get(get);
        Cell[] keyValues = result.rawCells();
        long timestamp = keyValues[0].getTimestamp();
        for (int i = 1; i < keyValues.length; ++i) {
            assertEquals(timestamp, keyValues[i].getTimestamp());
            byte[] qualifier = CellUtil.cloneQualifier(keyValues[i]);
            byte[] expectedValue = expectedValues.get(qualifier);
            if (expectedValue != null) {
                assertEquals(expectedValue, CellUtil.cloneValue(keyValues[i]));
            }
        }
        assertEquals(6, keyValues.length);

        // f1c1 f2c1 f2c2
        Get get2 = new Get(toBytes("Key1"));
        get2.addColumn(family1, family1_column1);
        get2.addColumn(family2, family2_column1);
        get2.addColumn(family2, family2_column2);
        Result result2 = multiCfHTable.get(get2);
        keyValues = result2.rawCells();
        timestamp = keyValues[0].getTimestamp();
        for (int i = 1; i < keyValues.length; ++i) {
            assertEquals(timestamp, keyValues[i].getTimestamp());
            byte[] qualifier = CellUtil.cloneQualifier(keyValues[i]);
            byte[] expectedValue = expectedValues.get(qualifier);
            if (expectedValue != null) {
                assertEquals(expectedValue, CellUtil.cloneValue(keyValues[i]));
            }
        }
        assertEquals(3, keyValues.length);

        //f2c1 f2c2
        Get get3 = new Get(toBytes("Key1"));
        get3.addFamily(family1);
        get3.addColumn(family2, family2_column1);
        get3.addColumn(family2, family2_column2);
        Result result3 = multiCfHTable.get(get3);
        keyValues = result3.rawCells();
        timestamp = keyValues[0].getTimestamp();
        for (int i = 1; i < keyValues.length; ++i) {
            assertEquals(timestamp, keyValues[i].getTimestamp());
            byte[] qualifier = CellUtil.cloneQualifier(keyValues[i]);
            byte[] expectedValue = expectedValues.get(qualifier);
            if (expectedValue != null) {
                assertEquals(expectedValue, CellUtil.cloneValue(keyValues[i]));
            }
        }
        assertEquals(5, keyValues.length);
    }

    @Test
    public void testMultiColumnFamilyDelete() throws Exception {
        String key1 = "scanKey1x";
        String key2 = "scanKey2x";
        String key3 = "scanKey3x";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";

        byte[] family1 = "family_with_group1".getBytes();
        byte[] family2 = "family_with_group2".getBytes();
        byte[] family3 = "family_with_group3".getBytes();

        byte[] family1_column1 = "family1_column1".getBytes();
        byte[] family1_column2 = "family1_column2".getBytes();
        byte[] family1_column3 = "family1_column3".getBytes();
        byte[] family2_column1 = "family2_column1".getBytes();
        byte[] family2_column2 = "family2_column2".getBytes();
        byte[] family2_column3 = "family2_column3".getBytes();
        byte[] family3_column1 = "family3_column1".getBytes();
        byte[] family1_value = "VVV1".getBytes();
        byte[] family2_value = "VVV2".getBytes();
        byte[] family3_value = "VVV3".getBytes();

        int rows = 10;

        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            Delete delete = new Delete(toBytes("Key" + i));
            multiCfHTable.delete(delete);
            put.addColumn(family1, family1_column1, family1_value);
            put.addColumn(family1, family1_column2, family1_value);
            put.addColumn(family1, family1_column3, family1_value);
            put.addColumn(family2, family2_column1, family2_value);
            put.addColumn(family2, family2_column2, family2_value);
            put.addColumn(family3, family3_column1, family3_value);
            multiCfHTable.put(put);
        }

        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        Delete delete = new Delete(toBytes("Key1"));
        delete.addColumns(family1, family1_column1);
        delete.addColumns(family2, family2_column1);
        multiCfHTable.delete(delete);
        // f1c2 f1c3 f2c2 f3c1
        Get get = new Get(toBytes("Key1"));
        Result result = multiCfHTable.get(get);
        Cell[] keyValues = result.rawCells();
        assertEquals(4, keyValues.length);
        assertFalse(result.containsColumn(family1, family1_column1));
        assertFalse(result.containsColumn(family2, family2_column1));

        assertTrue(result.containsColumn(family1, family1_column2));
        assertArrayEquals(result.getValue(family1, family1_column2), family1_value);
        assertTrue(result.containsColumn(family1, family1_column3));
        assertArrayEquals(result.getValue(family1, family1_column3), family1_value);
        assertTrue(result.containsColumn(family2, family2_column2));
        assertArrayEquals(result.getValue(family2, family2_column2), family2_value);
        assertTrue(result.containsColumn(family3, family3_column1));
        assertArrayEquals(result.getValue(family3, family3_column1), family3_value);

        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        delete = new Delete(toBytes("Key2"));
        delete.addFamily(family1);
        delete.addFamily(family2);
        // f3c1
        multiCfHTable.delete(delete);
        get = new Get(toBytes("Key2"));
        result = multiCfHTable.get(get);
        keyValues = result.rawCells();
        assertEquals(1, keyValues.length);

        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        delete = new Delete(toBytes("Key3"));
        delete.addFamily(family1);
        delete.addColumns(family2, family2_column1);
        multiCfHTable.delete(delete);
        // f2c2 f3c1
        get = new Get(toBytes("Key3"));
        result = multiCfHTable.get(get);
        keyValues = result.rawCells();
        assertEquals(2, keyValues.length);

        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        delete = new Delete(toBytes("Key4"));
        multiCfHTable.delete(delete);
        // null
        get = new Get(toBytes("Key4"));
        result = multiCfHTable.get(get);
        keyValues = result.rawCells();
        assertEquals(0, keyValues.length);

        // f1c1 f2c1 f2c2
        delete = new Delete(toBytes("Key5"));
        delete.addColumns(family1, family1_column2);
        delete.addColumns(family1, family1_column3);
        delete.addColumns(family3, family3_column1);
        multiCfHTable.delete(delete);
        // null
        get = new Get(toBytes("Key5"));
        result = multiCfHTable.get(get);
        keyValues = result.rawCells();
        assertEquals(3, keyValues.length);

        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            put.addColumn(family1, family1_column1, family1_value);
            put.addColumn(family1, family1_column2, family1_value);
            put.addColumn(family1, family1_column3, family1_value);
            put.addColumn(family2, family2_column1, family2_value);
            put.addColumn(family2, family2_column2, family2_value);
            put.addColumn(family3, family3_column1, family3_value);
            multiCfHTable.put(put);
        }

        delete = new Delete(toBytes("Key6"));
        delete.addColumn(family1, family1_column2);
        delete.addColumn(family2, family2_column1);
        multiCfHTable.delete(delete);
        get = new Get(toBytes("Key6"));
        result = multiCfHTable.get(get);
        keyValues = result.rawCells();
        assertEquals(6, keyValues.length);

        long lastTimestamp = result.getColumnCells(family1, family1_column1).get(0).getTimestamp();
        assertEquals(lastTimestamp, result.getColumnCells(family1, family1_column3).get(0)
            .getTimestamp());
        assertEquals(lastTimestamp, result.getColumnCells(family2, family2_column2).get(0)
            .getTimestamp());
        assertEquals(lastTimestamp, result.getColumnCells(family3, family3_column1).get(0)
            .getTimestamp());

        long oldTimestamp = result.getColumnCells(family1, family1_column2).get(0).getTimestamp();
        assertEquals(oldTimestamp, result.getColumnCells(family2, family2_column1).get(0)
            .getTimestamp());
        assertTrue(lastTimestamp > oldTimestamp);

        // delete previous data
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.addFamily(family1);
        deleteKey1Family.addFamily(family2);
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(family1);
        deleteKey2Family.addFamily(family2);
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.addFamily(family1);
        deleteKey3Family.addFamily(family2);

        multiCfHTable.delete(deleteKey1Family);
        multiCfHTable.delete(deleteKey2Family);
        multiCfHTable.delete(deleteKey3Family);

        long minTimeStamp = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp1 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp2 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp3 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp4 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp5 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp6 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp7 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp8 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp9 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp10 = System.currentTimeMillis();
        Thread.sleep(5);
        long timeStamp11 = System.currentTimeMillis();
        Thread.sleep(5);
        long maxTimeStamp = System.currentTimeMillis();

        Put putKey1Fam1Column1MinTs = new Put(toBytes(key1));
        putKey1Fam1Column1MinTs.addColumn(family1, family1_column1, minTimeStamp, toBytes(value1));

        Put putKey3Fam1Column1Ts1 = new Put(toBytes(key3));
        putKey3Fam1Column1Ts1.addColumn(family1, family1_column1, timeStamp1, toBytes(value2));

        Put putKey1Fam1Column2MinTs = new Put(toBytes(key1));
        putKey1Fam1Column2MinTs.addColumn(family1, family1_column2, minTimeStamp, toBytes(value1));

        Put putKey1Fam1Column2Ts3 = new Put(toBytes(key1));
        putKey1Fam1Column2Ts3.addColumn(family1, family1_column2, timeStamp3, toBytes(value2));

        Put putKey2Fam1Column2Ts3 = new Put(toBytes(key2));
        putKey2Fam1Column2Ts3.addColumn(family1, family1_column2, timeStamp3, toBytes(value2));

        Put putKey2Fam1Column3Ts1 = new Put(toBytes(key2));
        putKey2Fam1Column3Ts1.addColumn(family1, family1_column3, timeStamp1, toBytes(value2));

        Put putKey3Fam1Column3Ts1 = new Put(toBytes(key3));
        putKey3Fam1Column3Ts1.addColumn(family1, family1_column3, timeStamp1, toBytes(value2));

        Put putKey3Fam1Column2Ts6 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts6.addColumn(family1, family1_column2, timeStamp6, toBytes(value1));

        Put putKey2Fam1Column3Ts6 = new Put(toBytes(key2));
        putKey2Fam1Column3Ts6.addColumn(family1, family1_column3, timeStamp3, toBytes(value1));

        multiCfHTable.put(putKey1Fam1Column1MinTs);
        multiCfHTable.put(putKey3Fam1Column1Ts1);
        multiCfHTable.put(putKey1Fam1Column2MinTs);
        multiCfHTable.put(putKey1Fam1Column2Ts3);
        multiCfHTable.put(putKey2Fam1Column2Ts3);
        multiCfHTable.put(putKey2Fam1Column3Ts1);
        multiCfHTable.put(putKey3Fam1Column3Ts1);
        multiCfHTable.put(putKey3Fam1Column2Ts6);
        multiCfHTable.put(putKey2Fam1Column3Ts6);

        // test DeleteFamilyVersion single cf
        get = new Get(toBytes(key1));
        get.addFamily(family1);
        get.setTimeStamp(minTimeStamp);
        get.setMaxVersions(10);
        Result r = multiCfHTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        get = new Get(toBytes(key3));
        get.addFamily(family1);
        get.setTimeStamp(timeStamp1);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        get = new Get(toBytes(key2));
        get.addFamily(family1);
        get.setTimeStamp(timeStamp3);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        Delete delKey1MinTs = new Delete(toBytes(key1));
        delKey1MinTs.addFamilyVersion(family1, minTimeStamp);
        multiCfHTable.delete(delKey1MinTs);

        get = new Get(toBytes(key1));
        get.addFamily(family1);
        get.setTimeStamp(minTimeStamp);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        Delete delKey3Ts1 = new Delete(toBytes(key3));
        delKey3Ts1.addFamilyVersion(family1, timeStamp1);
        multiCfHTable.delete(delKey3Ts1);

        get = new Get(toBytes(key3));
        get.addFamily(family1);
        get.setTimeStamp(timeStamp1);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        Delete delKey2Ts3 = new Delete(toBytes(key2));
        delKey2Ts3.addFamilyVersion(family1, timeStamp3);
        multiCfHTable.delete(delKey2Ts3);

        get = new Get(toBytes(key2));
        get.addFamily(family1);
        get.setTimeStamp(timeStamp3);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        Scan scan = new Scan();
        scan.setStartRow(toBytes(key1));
        scan.setStopRow("scanKey4x".getBytes());
        scan.addFamily(family1);
        scan.setMaxVersions(10);
        ResultScanner scanner = multiCfHTable.getScanner(scan);
        int key1Cnt = 0, key2Cnt = 0, key3Cnt = 0;
        for (Result res : scanner) {
            for (Cell kv : res.rawCells()) {
                if (key1.equals(Bytes.toString(CellUtil.cloneRow(kv)))) {
                    ++key1Cnt;
                } else if (key2.equals(Bytes.toString(CellUtil.cloneRow(kv)))) {
                    ++key2Cnt;
                } else {
                    ++key3Cnt;
                }
            }
        }
        Assert.assertEquals(1, key1Cnt);
        Assert.assertEquals(1, key2Cnt);
        Assert.assertEquals(1, key3Cnt);

        multiCfHTable.delete(deleteKey1Family);
        multiCfHTable.delete(deleteKey2Family);
        multiCfHTable.delete(deleteKey3Family);

        // test DeleteFamilyVersion multiple cf
        Put putKey1Fam1Column3Ts6 = new Put(toBytes(key1));
        putKey1Fam1Column3Ts6.addColumn(family1, family1_column3, timeStamp6, toBytes(value3));

        Put putKey1Fam2Column2Ts2 = new Put(toBytes(key1));
        putKey1Fam2Column2Ts2.addColumn(family2, family2_column2, timeStamp2, toBytes(value1));

        Put putKey1Fam2Column3Ts2 = new Put(toBytes(key1));
        putKey1Fam2Column3Ts2.addColumn(family2, family2_column3, timeStamp2, toBytes(value1));

        Put putKey1Fam1Column2Ts1 = new Put(toBytes(key1));
        putKey1Fam1Column2Ts1.addColumn(family1, family1_column2, timeStamp1, toBytes(value2));

        Put putKey2Fam1Column2Ts8 = new Put(toBytes(key2));
        putKey2Fam1Column2Ts8.addColumn(family1, family1_column2, timeStamp8, toBytes(value2));

        Put putKey2Fam2Column3Ts1 = new Put(toBytes(key2));
        putKey2Fam2Column3Ts1.addColumn(family2, family2_column3, timeStamp3, toBytes(value3));

        Put putKey2Fam1Column1Ts1 = new Put(toBytes(key2));
        putKey2Fam1Column1Ts1.addColumn(family1, family1_column1, timeStamp8, toBytes(value1));

        Put putKey2Fam2Column1Ts3 = new Put(toBytes(key2));
        putKey2Fam2Column1Ts3.addColumn(family2, family2_column1, timeStamp3, toBytes(value2));

        Put putKey3Fam1Column2Ts9 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts9.addColumn(family1, family1_column2, timeStamp9, toBytes(value2));

        Put putKey3Fam2Column3Ts10 = new Put(toBytes(key3));
        putKey3Fam2Column3Ts10.addColumn(family2, family2_column3, timeStamp10, toBytes(value1));

        Put putKey3Fam2Column1Ts10 = new Put(toBytes(key3));
        putKey3Fam2Column1Ts10.addColumn(family2, family2_column1, timeStamp10, toBytes(value2));

        Put putKey3Fam1Column2Ts2 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts2.addColumn(family1, family1_column2, timeStamp2, toBytes(value1));

        multiCfHTable.put(putKey1Fam1Column3Ts6);
        multiCfHTable.put(putKey1Fam2Column2Ts2);
        multiCfHTable.put(putKey1Fam2Column3Ts2);
        multiCfHTable.put(putKey1Fam1Column2Ts1);
        multiCfHTable.put(putKey2Fam1Column2Ts8);
        multiCfHTable.put(putKey2Fam2Column3Ts1);
        multiCfHTable.put(putKey2Fam1Column1Ts1);
        multiCfHTable.put(putKey2Fam2Column1Ts3);
        multiCfHTable.put(putKey3Fam1Column2Ts9);
        multiCfHTable.put(putKey3Fam2Column3Ts10);
        multiCfHTable.put(putKey3Fam2Column1Ts10);
        multiCfHTable.put(putKey3Fam1Column2Ts2);

        Get getKey1 = new Get(toBytes(key1));
        getKey1.addFamily(family1);
        getKey1.addFamily(family2);
        getKey1.setMaxVersions(10);
        r = multiCfHTable.get(getKey1);
        Assert.assertEquals(4, r.rawCells().length);

        Get getKey2 = new Get(toBytes(key2));
        getKey2.addFamily(family1);
        getKey2.addFamily(family2);
        getKey2.setMaxVersions(10);
        r = multiCfHTable.get(getKey2);
        Assert.assertEquals(4, r.rawCells().length);

        Get getKey3 = new Get(toBytes(key3));
        getKey3.addFamily(family1);
        getKey3.addFamily(family2);
        getKey3.setMaxVersions(10);
        r = multiCfHTable.get(getKey3);
        Assert.assertEquals(4, r.rawCells().length);

        Delete delKey1Ts_6_2 = new Delete(toBytes(key1));
        delKey1Ts_6_2.addFamilyVersion(family1, timeStamp6);
        delKey1Ts_6_2.addFamilyVersion(family2, timeStamp2);
        multiCfHTable.delete(delKey1Ts_6_2);

        getKey1 = new Get(toBytes(key1));
        getKey1.addFamily(family1);
        getKey1.addFamily(family2);
        getKey1.setMaxVersions(10);
        r = multiCfHTable.get(getKey1);
        Assert.assertEquals(1, r.rawCells().length);
        for (Cell kv : r.rawCells()) {
            Assert.assertEquals(timeStamp1, kv.getTimestamp());
        }

        Delete delKey2Ts_8_3 = new Delete(toBytes(key2));
        delKey2Ts_8_3.addFamilyVersion(family1, timeStamp8);
        delKey2Ts_8_3.addFamilyVersion(family2, timeStamp3);
        multiCfHTable.delete(delKey2Ts_8_3);

        getKey2 = new Get(toBytes(key2));
        getKey2.addFamily(family1);
        getKey2.addFamily(family2);
        getKey2.setMaxVersions(10);
        r = multiCfHTable.get(getKey2);
        Assert.assertEquals(0, r.rawCells().length);

        Delete delKey3Ts_2_10 = new Delete(toBytes(key3));
        delKey3Ts_2_10.addFamilyVersion(family1, timeStamp2);
        delKey3Ts_2_10.addFamilyVersion(family2, timeStamp10);
        multiCfHTable.delete(delKey3Ts_2_10);

        getKey3 = new Get(toBytes(key3));
        getKey3.addFamily(family1);
        getKey3.addFamily(family2);
        getKey3.setMaxVersions(10);
        r = multiCfHTable.get(getKey3);
        Assert.assertEquals(1, r.rawCells().length);
        for (Cell kv : r.rawCells()) {
            Assert.assertEquals(timeStamp9, kv.getTimestamp());
        }

        scan = new Scan();
        scan.setStartRow(toBytes(key1));
        scan.setStopRow("scanKey4x".getBytes());
        scan.addFamily(family1);
        scan.addFamily(family2);
        scan.setMaxVersions(10);
        scanner = multiCfHTable.getScanner(scan);
        int ts1Cnt = 0, ts9Cnt = 0;
        for (Result res : scanner) {
            for (Cell kv : res.rawCells()) {
                if (kv.getTimestamp() == timeStamp1) {
                    ++ts1Cnt;
                } else if (kv.getTimestamp() == timeStamp9) {
                    ++ts9Cnt;
                }
            }
        }
        Assert.assertEquals(1, ts1Cnt);
        Assert.assertEquals(1, ts9Cnt);

        multiCfHTable.delete(deleteKey1Family);
        multiCfHTable.delete(deleteKey2Family);
        multiCfHTable.delete(deleteKey3Family);
    }
}
