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

import org.apache.hadoop.conf.Configuration;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.util.*;

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
        deleteKey1Family.deleteFamily(toBytes(family1));
        deleteKey1Family.deleteFamily(toBytes(family2));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.deleteFamily(toBytes(family1));
        deleteKey2Family.deleteFamily(toBytes(family2));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.deleteFamily(toBytes(family1));
        deleteKey3Family.deleteFamily(toBytes(family2));

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
        putKey1Fam1Column1MinTs.add(toBytes(family1), toBytes(column1), minTimeStamp,
            toBytes(value1));

        Put putKey3Fam1Column1Ts1 = new Put(toBytes(key3));
        putKey3Fam1Column1Ts1.add(toBytes(family1), toBytes(column1), timeStamp1, toBytes(value2));

        Put putKey1Fam1Column2MinTs = new Put(toBytes(key1));
        putKey1Fam1Column2MinTs.add(toBytes(family1), toBytes(column2), minTimeStamp,
            toBytes(value1));

        Put putKey1Fam1Column2Ts3 = new Put(toBytes(key1));
        putKey1Fam1Column2Ts3.add(toBytes(family1), toBytes(column2), timeStamp3, toBytes(value2));

        Put putKey2Fam1Column2Ts3 = new Put(toBytes(key2));
        putKey2Fam1Column2Ts3.add(toBytes(family1), toBytes(column2), timeStamp3, toBytes(value2));

        Put putKey2Fam1Column3Ts1 = new Put(toBytes(key2));
        putKey2Fam1Column3Ts1.add(toBytes(family1), toBytes(column3), timeStamp1, toBytes(value2));

        Put putKey3Fam1Column3Ts1 = new Put(toBytes(key3));
        putKey3Fam1Column3Ts1.add(toBytes(family1), toBytes(column3), timeStamp1, toBytes(value2));

        Put putKey3Fam1Column2Ts4 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts4.add(toBytes(family1), toBytes(column2), timeStamp4, toBytes(value1));

        Put putKey2Fam1Column3Ts3 = new Put(toBytes(key2));
        putKey2Fam1Column3Ts3.add(toBytes(family1), toBytes(column3), timeStamp3, toBytes(value1));

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
        Assert.assertEquals(2, r.raw().length);

        get = new Get(toBytes(key3));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(timeStamp1);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        get = new Get(toBytes(key2));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(timeStamp3);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        Delete delKey1MinTs = new Delete(toBytes(key1));
        delKey1MinTs.deleteFamilyVersion(toBytes(family1), minTimeStamp);
        multiCfHTable.delete(delKey1MinTs);

        get = new Get(toBytes(key1));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(minTimeStamp);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        Delete delKey3Ts1 = new Delete(toBytes(key3));
        delKey3Ts1.deleteFamilyVersion(toBytes(family1), timeStamp1);
        multiCfHTable.delete(delKey3Ts1);

        get = new Get(toBytes(key3));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(timeStamp1);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        Delete delKey2Ts3 = new Delete(toBytes(key2));
        delKey2Ts3.deleteFamilyVersion(family1.getBytes(), timeStamp3);
        multiCfHTable.delete(delKey2Ts3);

        get = new Get(toBytes(key2));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(timeStamp3);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        Scan scan = new Scan();
        scan.setStartRow(toBytes(key1));
        scan.setStopRow("scanKey4x".getBytes());
        scan.addFamily(toBytes(family1));
        scan.setMaxVersions(10);
        ResultScanner scanner = multiCfHTable.getScanner(scan);
        int key1Cnt = 0, key2Cnt = 0, key3Cnt = 0;
        for (Result result : scanner) {
            for (KeyValue kv : result.raw()) {
                if (key1.equals(Bytes.toString(kv.getRow()))) {
                    ++key1Cnt;
                } else if (key2.equals(Bytes.toString(kv.getRow()))) {
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
        putKey1Fam1Column3Ts4.add(toBytes(family1), toBytes(column3), timeStamp4, toBytes(value3));

        Put putKey1Fam2Column2Ts2 = new Put(toBytes(key1));
        putKey1Fam2Column2Ts2.add(toBytes(family2), toBytes(column2), timeStamp2, toBytes(value1));

        Put putKey1Fam2Column3Ts2 = new Put(toBytes(key1));
        putKey1Fam2Column3Ts2.add(toBytes(family2), toBytes(column3), timeStamp2, toBytes(value1));

        Put putKey1Fam1Column2Ts1 = new Put(toBytes(key1));
        putKey1Fam1Column2Ts1.add(toBytes(family1), toBytes(column2), timeStamp1, toBytes(value2));

        Put putKey2Fam1Column2Ts5 = new Put(toBytes(key2));
        putKey2Fam1Column2Ts5.add(toBytes(family1), toBytes(column2), timeStamp5, toBytes(value2));

        Put putKey2Fam2Column3Ts1 = new Put(toBytes(key2));
        putKey2Fam2Column3Ts1.add(toBytes(family2), toBytes(column3), timeStamp3, toBytes(value3));

        Put putKey2Fam1Column1Ts5 = new Put(toBytes(key2));
        putKey2Fam1Column1Ts5.add(toBytes(family1), toBytes(column1), timeStamp5, toBytes(value1));

        Put putKey2Fam2Column1Ts3 = new Put(toBytes(key2));
        putKey2Fam2Column1Ts3.add(toBytes(family2), toBytes(column1), timeStamp3, toBytes(value2));

        Put putKey3Fam1Column2Ts6 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts6.add(toBytes(family1), toBytes(column2), timeStamp6, toBytes(value2));

        Put putKey3Fam2Column3Ts7 = new Put(toBytes(key3));
        putKey3Fam2Column3Ts7.add(toBytes(family2), toBytes(column3), timeStamp7, toBytes(value1));

        Put putKey3Fam2Column1Ts7 = new Put(toBytes(key3));
        putKey3Fam2Column1Ts7.add(toBytes(family2), toBytes(column1), timeStamp7, toBytes(value2));

        Put putKey3Fam1Column2Ts2 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts2.add(toBytes(family1), toBytes(column2), timeStamp2, toBytes(value1));

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
        Assert.assertEquals(4, r.raw().length);

        Get getKey2 = new Get(toBytes(key2));
        getKey2.addFamily(toBytes(family1));
        getKey2.addFamily(toBytes(family2));
        getKey2.setMaxVersions(10);
        r = multiCfHTable.get(getKey2);
        Assert.assertEquals(4, r.raw().length);

        Get getKey3 = new Get(toBytes(key3));
        getKey3.addFamily(toBytes(family1));
        getKey3.addFamily(toBytes(family2));
        getKey3.setMaxVersions(10);
        r = multiCfHTable.get(getKey3);
        Assert.assertEquals(4, r.raw().length);

        Delete delKey1Ts_6_2 = new Delete(toBytes(key1));
        delKey1Ts_6_2.deleteFamilyVersion(toBytes(family1), timeStamp4);
        delKey1Ts_6_2.deleteFamilyVersion(toBytes(family2), timeStamp2);
        multiCfHTable.delete(delKey1Ts_6_2);

        getKey1 = new Get(toBytes(key1));
        getKey1.addFamily(toBytes(family1));
        getKey1.addFamily(toBytes(family2));
        getKey1.setMaxVersions(10);
        r = multiCfHTable.get(getKey1);
        Assert.assertEquals(1, r.raw().length);
        for (KeyValue kv : r.raw()) {
            Assert.assertEquals(timeStamp1, kv.getTimestamp());
        }

        Delete delKey2Ts_5_3 = new Delete(toBytes(key2));
        delKey2Ts_5_3.deleteFamilyVersion(toBytes(family1), timeStamp5);
        delKey2Ts_5_3.deleteFamilyVersion(toBytes(family2), timeStamp3);
        multiCfHTable.delete(delKey2Ts_5_3);

        getKey2 = new Get(toBytes(key2));
        getKey2.addFamily(toBytes(family1));
        getKey2.addFamily(toBytes(family2));
        getKey2.setMaxVersions(10);
        r = multiCfHTable.get(getKey2);
        Assert.assertEquals(0, r.raw().length);

        Delete delKey3Ts_2_7 = new Delete(toBytes(key3));
        delKey3Ts_2_7.deleteFamilyVersion(toBytes(family1), timeStamp2);
        delKey3Ts_2_7.deleteFamilyVersion(toBytes(family2), timeStamp7);
        multiCfHTable.delete(delKey3Ts_2_7);

        getKey3 = new Get(toBytes(key3));
        getKey3.addFamily(toBytes(family1));
        getKey3.addFamily(toBytes(family2));
        getKey3.setMaxVersions(10);
        r = multiCfHTable.get(getKey3);
        Assert.assertEquals(1, r.raw().length);
        for (KeyValue kv : r.raw()) {
            Assert.assertEquals(timeStamp6, kv.getTimestamp());
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
            for (KeyValue kv : result.raw()) {
                if (kv.getTimestamp() == timeStamp1) {
                    ++ts1Cnt;
                } else if (kv.getTimestamp() == timeStamp6) {
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
    public void testMulfiColumnFamilyBufferedMutator() throws Exception {
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
            put.add(family1, family1_column1, family1_value);
            put.add(family1, family1_column2, family1_value);
            put.add(family1, family1_column3, family1_value);
            put.add(family2, family2_column1, family2_value);
            put.add(family2, family2_column2, family2_value);
            put.add(family3, family3_column1, family3_value);
            mutations.add(put);
        }
        mutator.mutate(mutations);

        // test force flush
        mutator.flush();
        Get get = new Get(toBytes("Key2"));
        get.addFamily(family1);
        get.addFamily(family2);
        Result result = multiCfHTable.get(get);
        Assert.assertEquals(5, result.raw().length);

        mutations.clear();
        for (int i = 0; i < rows; ++i) {
            if (i % 5 == 0) { // 0, 5
                Delete delete = new Delete(toBytes("Key" + i));
                delete.deleteFamily(family2);
                delete.deleteFamily(family3);
                mutations.add(delete);
            }
        }
        mutator.mutate(mutations);
        mutator.flush();

        get = new Get(toBytes("Key0"));
        result = multiCfHTable.get(get);
        Assert.assertEquals(3, result.raw().length);
        Assert.assertFalse(result.containsColumn(family2, family2_column1));
        Assert.assertFalse(result.containsColumn(family2, family2_column2));
        Assert.assertFalse(result.containsColumn(family3, family3_column1));

        get = new Get(toBytes("Key5"));
        result = multiCfHTable.get(get);
        Assert.assertEquals(3, result.raw().length);
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
            count += r.raw().length;
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
                put.add(family1, family1_column1, family1_value);
                put.add(family1, family1_column2, family1_value);
                put.add(family1, family1_column3, family1_value);
                put.add(family2, family2_column1, family2_value);
                put.add(family3, family3_column1, family2_value);
                put.add(family3, family3_column2, family3_value);
                mutations.add(put);
                if (i % 3 == 0) { // 0, 3, 6, 9
                    Delete delete = new Delete(toBytes(keys.get(i)));
                    delete.deleteFamily(family1);
                    delete.deleteFamily(family2);
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
        Assert.assertEquals(6 , result.raw().length);
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
        Assert.assertEquals(2, result.raw().length);
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
        Assert.assertEquals(2, result.raw().length);
        Assert.assertFalse(result.containsColumn(family1, family1_column1));
        Assert.assertFalse(result.containsColumn(family1, family1_column2));
        Assert.assertFalse(result.containsColumn(family1, family1_column3));
        Assert.assertFalse(result.containsColumn(family2, family2_column1));
        Assert.assertTrue(result.containsColumn(family3, family3_column1));
        Assert.assertTrue(result.containsColumn(family3, family3_column2));

        // clean data
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
            count += r.raw().length;
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
            put.add(family1, family1_column1, family1_value);
            put.add(family1, family1_column2, family1_value);
            put.add(family1, family1_column3, family1_value);
            put.add(family2, family2_column1, family2_value);
            put.add(family2, family2_column2, family2_value);
            put.add(family3, family3_column1, family3_value);
            batchLsit.add(put);
        }

        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        Delete delete = new Delete(toBytes("Key1"));
        delete.deleteColumns(family1, family1_column1);
        delete.deleteColumns(family2, family2_column1);
        batchLsit.add(delete);
        multiCfHTable.batch(batchLsit);
        // f1c2 f1c3 f2c2 f3c1
        Get get = new Get(toBytes("Key1"));
        Result result = multiCfHTable.get(get);
        KeyValue[] keyValues = result.raw();
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
        delete.deleteColumns(family1, family1_column2);
        delete.deleteColumns(family1, family1_column3);
        delete.deleteColumns(family3, family3_column1);
        batchLsit.add(delete);
        // null
        multiCfHTable.batch(batchLsit);
        get = new Get(toBytes("Key2"));
        result = multiCfHTable.get(get);
        keyValues = result.raw();
        assertEquals(3, keyValues.length);
        batchLsit.clear();
        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            put.add(family1, family1_column1, family1_value);
            put.add(family1, family1_column2, family1_value);
            put.add(family1, family1_column3, family1_value);
            put.add(family2, family2_column1, family2_value);
            put.add(family2, family2_column2, family2_value);
            put.add(family3, family3_column1, family3_value);
            batchLsit.add(put);
        }

        delete = new Delete(toBytes("Key3"));
        delete.deleteColumn(family1, family1_column2);
        delete.deleteColumn(family2, family2_column1);
        batchLsit.add(delete);
        multiCfHTable.batch(batchLsit);
        get = new Get(toBytes("Key3"));
        result = multiCfHTable.get(get);
        keyValues = result.raw();
        assertEquals(6, keyValues.length);

        batchLsit.clear();
        delete = new Delete(toBytes("Key4"));
        delete.deleteColumns(family1, family1_column2);
        delete.deleteColumns(family2, family2_column1);
        delete.deleteFamily(family3);
        batchLsit.add(delete);
        multiCfHTable.batch(batchLsit);
        get = new Get(toBytes("Key4"));
        get.setMaxVersions(10);
        result = multiCfHTable.get(get);
        keyValues = result.raw();
        assertEquals(6, keyValues.length);

        batchLsit.clear();
        final long[] updateCounter = new long[] { 0L };
        delete = new Delete(toBytes("Key5"));
        delete.deleteColumns(family1, family1_column2);
        delete.deleteColumns(family2, family2_column1);
        delete.deleteFamily(family3);
        batchLsit.add(delete);
        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            put.add(family1, family1_column1, family1_value);
            put.add(family1, family1_column2, family1_value);
            put.add(family1, family1_column3, family1_value);
            put.add(family2, family2_column1, family2_value);
            put.add(family2, family2_column2, family2_value);
            put.add(family3, family3_column1, family3_value);
            batchLsit.add(put);
        }
        multiCfHTable.batchCallback(batchLsit, new Batch.Callback<MutationResult>() {
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
            put.add(family1, family1_column1, family1_value);
            put.add(family1, family1_column2, family1_value);
            put.add(family1, family1_column3, family1_value);
            put.add(family2, family2_column1, family2_value);
            put.add(family2, family2_column2, family2_value);
            put.add(family3, family3_column1, family3_value);
            multiCfHTable.put(put);
        }

        Scan scan = new Scan();
        scan.setStartRow(toBytes("Key"));
        scan.setStopRow(toBytes("Kf"));
        ResultScanner scanner = multiCfHTable.getScanner(scan);
        int count = 0;

        for (Result result : scanner) {
            KeyValue[] keyValues = result.raw();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = keyValues[i].getQualifier();
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, keyValues[i].getValue());
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
            KeyValue[] keyValues = result.raw();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = keyValues[i].getQualifier();
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, keyValues[i].getValue());
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
            put.add(family1, family1_column1, family1_value);
            put.add(family1, family1_column2, family1_value);
            put.add(family1, family1_column3, family1_value);
            put.add(family2, family2_column1, family2_value);
            put.add(family2, family2_column2, family2_value);
            put.add(family3, family3_column1, family3_value);
            multiCfHTable.put(put);
        }

        Scan scan = new Scan();
        scan.addFamily(family1);
        scan.addFamily(family2);
        scan.setReversed(true);
        ResultScanner scanner2 = multiCfHTable.getScanner(scan);

        for (Result result : scanner2) {
            KeyValue[] keyValues = result.raw();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = keyValues[i].getQualifier();
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, keyValues[i].getValue());
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
            put.add(family1, family1_column1, family1_value);
            put.add(family1, family1_column2, family1_value);
            put.add(family1, family1_column3, family1_value);
            put.add(family2, family2_column1, family2_value);
            put.add(family2, family2_column2, family2_value);
            put.add(family3, family3_column1, family3_value);
            multiCfHTable.put(put);
        }

        Scan scan = new Scan();
        scan.setStartRow(toBytes("Key"));
        scan.setStopRow(toBytes("Kf"));
        scan.addColumn(family1, family1_column1);
        scan.addColumn(family2, family2_column1);
        ResultScanner scanner = multiCfHTable.getScanner(scan);

        for (Result result : scanner) {
            KeyValue[] keyValues = result.raw();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = keyValues[i].getQualifier();
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, keyValues[i].getValue());
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
            KeyValue[] keyValues = result.raw();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = keyValues[i].getQualifier();
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, keyValues[i].getValue());
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
            KeyValue[] keyValues = result.raw();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = keyValues[i].getQualifier();
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, keyValues[i].getValue());
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
            KeyValue[] keyValues = result.raw();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = keyValues[i].getQualifier();
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, keyValues[i].getValue());
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
            put.add(family1, family1_column1, family1_value);
            put.add(family1, family1_column2, family1_value);
            put.add(family1, family1_column3, family1_value);
            put.add(family2, family2_column1, family2_value);
            put.add(family2, family2_column2, family2_value);
            put.add(family3, family3_column1, family3_value);
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
            KeyValue[] keyValues = result.raw();
            long timestamp = keyValues[0].getTimestamp();
            for (int i = 1; i < keyValues.length; ++i) {
                assertEquals(timestamp, keyValues[i].getTimestamp());
                byte[] qualifier = keyValues[i].getQualifier();
                byte[] expectedValue = expectedValues.get(qualifier);
                if (expectedValue != null) {
                    assertEquals(expectedValue, keyValues[i].getValue());
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
            put.add(family1, family1_column1, family1_value);
            put.add(family1, family1_column2, family1_value);
            put.add(family1, family1_column3, family1_value);
            put.add(family2, family2_column1, family2_value);
            put.add(family2, family2_column2, family2_value);
            put.add(family3, family3_column1, family3_value);
            multiCfHTable.put(put);
        }

        // get with empty family
        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        Get get = new Get(toBytes("Key1"));
        Result result = multiCfHTable.get(get);
        KeyValue[] keyValues = result.raw();
        long timestamp = keyValues[0].getTimestamp();
        for (int i = 1; i < keyValues.length; ++i) {
            assertEquals(timestamp, keyValues[i].getTimestamp());
            byte[] qualifier = keyValues[i].getQualifier();
            byte[] expectedValue = expectedValues.get(qualifier);
            if (expectedValue != null) {
                assertEquals(expectedValue, keyValues[i].getValue());
            }
        }
        assertEquals(6, keyValues.length);

        // f1c1 f2c1 f2c2
        Get get2 = new Get(toBytes("Key1"));
        get2.addColumn(family1, family1_column1);
        get2.addColumn(family2, family2_column1);
        get2.addColumn(family2, family2_column2);
        Result result2 = multiCfHTable.get(get2);
        keyValues = result2.raw();
        timestamp = keyValues[0].getTimestamp();
        for (int i = 1; i < keyValues.length; ++i) {
            assertEquals(timestamp, keyValues[i].getTimestamp());
            byte[] qualifier = keyValues[i].getQualifier();
            byte[] expectedValue = expectedValues.get(qualifier);
            if (expectedValue != null) {
                assertEquals(expectedValue, keyValues[i].getValue());
            }
        }
        assertEquals(3, keyValues.length);

        //f2c1 f2c2
        Get get3 = new Get(toBytes("Key1"));
        get3.addFamily(family1);
        get3.addColumn(family2, family2_column1);
        get3.addColumn(family2, family2_column2);
        Result result3 = multiCfHTable.get(get3);
        keyValues = result3.raw();
        timestamp = keyValues[0].getTimestamp();
        for (int i = 1; i < keyValues.length; ++i) {
            assertEquals(timestamp, keyValues[i].getTimestamp());
            byte[] qualifier = keyValues[i].getQualifier();
            byte[] expectedValue = expectedValues.get(qualifier);
            if (expectedValue != null) {
                assertEquals(expectedValue, keyValues[i].getValue());
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
            put.add(family1, family1_column1, family1_value);
            put.add(family1, family1_column2, family1_value);
            put.add(family1, family1_column3, family1_value);
            put.add(family2, family2_column1, family2_value);
            put.add(family2, family2_column2, family2_value);
            put.add(family3, family3_column1, family3_value);
            multiCfHTable.put(put);
        }

        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        Delete delete = new Delete(toBytes("Key1"));
        delete.deleteColumns(family1, family1_column1);
        delete.deleteColumns(family2, family2_column1);
        multiCfHTable.delete(delete);
        // f1c2 f1c3 f2c2 f3c1
        Get get = new Get(toBytes("Key1"));
        Result result = multiCfHTable.get(get);
        KeyValue[] keyValues = result.raw();
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
        delete.deleteFamily(family1);
        delete.deleteFamily(family2);
        // f3c1
        multiCfHTable.delete(delete);
        get = new Get(toBytes("Key2"));
        result = multiCfHTable.get(get);
        keyValues = result.raw();
        assertEquals(1, keyValues.length);

        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        delete = new Delete(toBytes("Key3"));
        delete.deleteFamily(family1);
        delete.deleteColumns(family2, family2_column1);
        multiCfHTable.delete(delete);
        // f2c2 f3c1
        get = new Get(toBytes("Key3"));
        result = multiCfHTable.get(get);
        keyValues = result.raw();
        assertEquals(2, keyValues.length);

        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        delete = new Delete(toBytes("Key4"));
        multiCfHTable.delete(delete);
        // null
        get = new Get(toBytes("Key4"));
        result = multiCfHTable.get(get);
        keyValues = result.raw();
        assertEquals(0, keyValues.length);

        // f1c1 f2c1 f2c2
        delete = new Delete(toBytes("Key5"));
        delete.deleteColumns(family1, family1_column2);
        delete.deleteColumns(family1, family1_column3);
        delete.deleteColumns(family3, family3_column1);
        multiCfHTable.delete(delete);
        // null
        get = new Get(toBytes("Key5"));
        result = multiCfHTable.get(get);
        keyValues = result.raw();
        assertEquals(3, keyValues.length);

        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            put.add(family1, family1_column1, family1_value);
            put.add(family1, family1_column2, family1_value);
            put.add(family1, family1_column3, family1_value);
            put.add(family2, family2_column1, family2_value);
            put.add(family2, family2_column2, family2_value);
            put.add(family3, family3_column1, family3_value);
            multiCfHTable.put(put);
        }

        delete = new Delete(toBytes("Key6"));
        delete.deleteColumn(family1, family1_column2);
        delete.deleteColumn(family2, family2_column1);
        multiCfHTable.delete(delete);
        get = new Get(toBytes("Key6"));
        result = multiCfHTable.get(get);
        keyValues = result.raw();
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
        deleteKey1Family.deleteFamily(family1);
        deleteKey1Family.deleteFamily(family2);
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.deleteFamily(family1);
        deleteKey2Family.deleteFamily(family2);
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.deleteFamily(family1);
        deleteKey3Family.deleteFamily(family2);

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
        putKey1Fam1Column1MinTs.add(family1, family1_column1, minTimeStamp, toBytes(value1));

        Put putKey3Fam1Column1Ts1 = new Put(toBytes(key3));
        putKey3Fam1Column1Ts1.add(family1, family1_column1, timeStamp1, toBytes(value2));

        Put putKey1Fam1Column2MinTs = new Put(toBytes(key1));
        putKey1Fam1Column2MinTs.add(family1, family1_column2, minTimeStamp, toBytes(value1));

        Put putKey1Fam1Column2Ts3 = new Put(toBytes(key1));
        putKey1Fam1Column2Ts3.add(family1, family1_column2, timeStamp3, toBytes(value2));

        Put putKey2Fam1Column2Ts3 = new Put(toBytes(key2));
        putKey2Fam1Column2Ts3.add(family1, family1_column2, timeStamp3, toBytes(value2));

        Put putKey2Fam1Column3Ts1 = new Put(toBytes(key2));
        putKey2Fam1Column3Ts1.add(family1, family1_column3, timeStamp1, toBytes(value2));

        Put putKey3Fam1Column3Ts1 = new Put(toBytes(key3));
        putKey3Fam1Column3Ts1.add(family1, family1_column3, timeStamp1, toBytes(value2));

        Put putKey3Fam1Column2Ts6 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts6.add(family1, family1_column2, timeStamp6, toBytes(value1));

        Put putKey2Fam1Column3Ts6 = new Put(toBytes(key2));
        putKey2Fam1Column3Ts6.add(family1, family1_column3, timeStamp3, toBytes(value1));

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
        Assert.assertEquals(2, r.raw().length);

        get = new Get(toBytes(key3));
        get.addFamily(family1);
        get.setTimeStamp(timeStamp1);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        get = new Get(toBytes(key2));
        get.addFamily(family1);
        get.setTimeStamp(timeStamp3);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        Delete delKey1MinTs = new Delete(toBytes(key1));
        delKey1MinTs.deleteFamilyVersion(family1, minTimeStamp);
        multiCfHTable.delete(delKey1MinTs);

        get = new Get(toBytes(key1));
        get.addFamily(family1);
        get.setTimeStamp(minTimeStamp);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        Delete delKey3Ts1 = new Delete(toBytes(key3));
        delKey3Ts1.deleteFamilyVersion(family1, timeStamp1);
        multiCfHTable.delete(delKey3Ts1);

        get = new Get(toBytes(key3));
        get.addFamily(family1);
        get.setTimeStamp(timeStamp1);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        Delete delKey2Ts3 = new Delete(toBytes(key2));
        delKey2Ts3.deleteFamilyVersion(family1, timeStamp3);
        multiCfHTable.delete(delKey2Ts3);

        get = new Get(toBytes(key2));
        get.addFamily(family1);
        get.setTimeStamp(timeStamp3);
        get.setMaxVersions(10);
        r = multiCfHTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        Scan scan = new Scan();
        scan.setStartRow(toBytes(key1));
        scan.setStopRow("scanKey4x".getBytes());
        scan.addFamily(family1);
        scan.setMaxVersions(10);
        ResultScanner scanner = multiCfHTable.getScanner(scan);
        int key1Cnt = 0, key2Cnt = 0, key3Cnt = 0;
        for (Result res : scanner) {
            for (KeyValue kv : res.raw()) {
                if (key1.equals(Bytes.toString(kv.getRow()))) {
                    ++key1Cnt;
                } else if (key2.equals(Bytes.toString(kv.getRow()))) {
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
        putKey1Fam1Column3Ts6.add(family1, family1_column3, timeStamp6, toBytes(value3));

        Put putKey1Fam2Column2Ts2 = new Put(toBytes(key1));
        putKey1Fam2Column2Ts2.add(family2, family2_column2, timeStamp2, toBytes(value1));

        Put putKey1Fam2Column3Ts2 = new Put(toBytes(key1));
        putKey1Fam2Column3Ts2.add(family2, family2_column3, timeStamp2, toBytes(value1));

        Put putKey1Fam1Column2Ts1 = new Put(toBytes(key1));
        putKey1Fam1Column2Ts1.add(family1, family1_column2, timeStamp1, toBytes(value2));

        Put putKey2Fam1Column2Ts8 = new Put(toBytes(key2));
        putKey2Fam1Column2Ts8.add(family1, family1_column2, timeStamp8, toBytes(value2));

        Put putKey2Fam2Column3Ts1 = new Put(toBytes(key2));
        putKey2Fam2Column3Ts1.add(family2, family2_column3, timeStamp3, toBytes(value3));

        Put putKey2Fam1Column1Ts1 = new Put(toBytes(key2));
        putKey2Fam1Column1Ts1.add(family1, family1_column1, timeStamp8, toBytes(value1));

        Put putKey2Fam2Column1Ts3 = new Put(toBytes(key2));
        putKey2Fam2Column1Ts3.add(family2, family2_column1, timeStamp3, toBytes(value2));

        Put putKey3Fam1Column2Ts9 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts9.add(family1, family1_column2, timeStamp9, toBytes(value2));

        Put putKey3Fam2Column3Ts10 = new Put(toBytes(key3));
        putKey3Fam2Column3Ts10.add(family2, family2_column3, timeStamp10, toBytes(value1));

        Put putKey3Fam2Column1Ts10 = new Put(toBytes(key3));
        putKey3Fam2Column1Ts10.add(family2, family2_column1, timeStamp10, toBytes(value2));

        Put putKey3Fam1Column2Ts2 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts2.add(family1, family1_column2, timeStamp2, toBytes(value1));

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
        Assert.assertEquals(4, r.raw().length);

        Get getKey2 = new Get(toBytes(key2));
        getKey2.addFamily(family1);
        getKey2.addFamily(family2);
        getKey2.setMaxVersions(10);
        r = multiCfHTable.get(getKey2);
        Assert.assertEquals(4, r.raw().length);

        Get getKey3 = new Get(toBytes(key3));
        getKey3.addFamily(family1);
        getKey3.addFamily(family2);
        getKey3.setMaxVersions(10);
        r = multiCfHTable.get(getKey3);
        Assert.assertEquals(4, r.raw().length);

        Delete delKey1Ts_6_2 = new Delete(toBytes(key1));
        delKey1Ts_6_2.deleteFamilyVersion(family1, timeStamp6);
        delKey1Ts_6_2.deleteFamilyVersion(family2, timeStamp2);
        multiCfHTable.delete(delKey1Ts_6_2);

        getKey1 = new Get(toBytes(key1));
        getKey1.addFamily(family1);
        getKey1.addFamily(family2);
        getKey1.setMaxVersions(10);
        r = multiCfHTable.get(getKey1);
        Assert.assertEquals(1, r.raw().length);
        for (KeyValue kv : r.raw()) {
            Assert.assertEquals(timeStamp1, kv.getTimestamp());
        }

        Delete delKey2Ts_8_3 = new Delete(toBytes(key2));
        delKey2Ts_8_3.deleteFamilyVersion(family1, timeStamp8);
        delKey2Ts_8_3.deleteFamilyVersion(family2, timeStamp3);
        multiCfHTable.delete(delKey2Ts_8_3);

        getKey2 = new Get(toBytes(key2));
        getKey2.addFamily(family1);
        getKey2.addFamily(family2);
        getKey2.setMaxVersions(10);
        r = multiCfHTable.get(getKey2);
        Assert.assertEquals(0, r.raw().length);

        Delete delKey3Ts_2_10 = new Delete(toBytes(key3));
        delKey3Ts_2_10.deleteFamilyVersion(family1, timeStamp2);
        delKey3Ts_2_10.deleteFamilyVersion(family2, timeStamp10);
        multiCfHTable.delete(delKey3Ts_2_10);

        getKey3 = new Get(toBytes(key3));
        getKey3.addFamily(family1);
        getKey3.addFamily(family2);
        getKey3.setMaxVersions(10);
        r = multiCfHTable.get(getKey3);
        Assert.assertEquals(1, r.raw().length);
        for (KeyValue kv : r.raw()) {
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
            for (KeyValue kv : res.raw()) {
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

    @Test
    public void testFamilyFilter() throws Exception {
        String key1 = "getKey1";
        String key2 = "getKey2";
        String column1 = "abc";
        String column2 = "def";
        String value1 = "value1";
        String value2 = "value2";
        String family1 = "family_with_group1";
        String family2 = "family_with_group2";
        String family3 = "family_with_group3";

        Delete deleteKey1 = new Delete(toBytes(key1));
        deleteKey1.deleteFamily(toBytes(family1));
        deleteKey1.deleteFamily(toBytes(family2));
        deleteKey1.deleteFamily(toBytes(family3));
        Delete deleteKey2 = new Delete(toBytes(key2));
        deleteKey2.deleteFamily(toBytes(family1));
        deleteKey2.deleteFamily(toBytes(family2));
        deleteKey2.deleteFamily(toBytes(family3));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.add(toBytes(family1), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.add(toBytes(family2), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.add(toBytes(family2), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.add(toBytes(family3), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.add(toBytes(family3), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.add(toBytes(family3), toBytes(column1), toBytes(value2));

       multiCfHTable.delete(deleteKey1);
       multiCfHTable.delete(deleteKey2);
       multiCfHTable.put(putKey1Column1Value1);
       multiCfHTable.put(putKey1Column1Value2);
       multiCfHTable.put(putKey1Column2Value2);
       multiCfHTable.put(putKey2Column2Value1);
       multiCfHTable.put(putKey2Column1Value1);
       multiCfHTable.put(putKey2Column1Value2);

        Scan scan;
        scan = new Scan();
        scan.addFamily(family1.getBytes());
        scan.addFamily(family2.getBytes());
        scan.addFamily(family3.getBytes());
        scan.setMaxVersions(10);
        FamilyFilter f = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(family2)));
        scan.setFilter(f);
        ResultScanner scanner = multiCfHTable.getScanner(scan);

        int res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(keyValue.getFamily()),
                        Bytes.toString(keyValue.getQualifier()),
                        keyValue.getTimestamp(),
                        Bytes.toString(keyValue.getValue())
                );
                Assert.assertArrayEquals(family2.getBytes(), keyValue.getFamily());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 2);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family1.getBytes());
        scan.addFamily(family2.getBytes());
        scan.addFamily(family3.getBytes());
        scan.setMaxVersions(10);
        f = new FamilyFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(family2)));
        scan.setFilter(f);
        scanner = multiCfHTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(keyValue.getFamily()),
                        Bytes.toString(keyValue.getQualifier()),
                        keyValue.getTimestamp(),
                        Bytes.toString(keyValue.getValue())
                );
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 4);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family1.getBytes());
        scan.addFamily(family2.getBytes());
        scan.addFamily(family3.getBytes());
        scan.setMaxVersions(10);
        f = new FamilyFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(family2)));
        scan.setFilter(f);
        scanner = multiCfHTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(keyValue.getFamily()),
                        Bytes.toString(keyValue.getQualifier()),
                        keyValue.getTimestamp(),
                        Bytes.toString(keyValue.getValue())
                );
                Assert.assertArrayEquals(family3.getBytes(), keyValue.getFamily());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 3);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family1.getBytes());
        scan.addFamily(family2.getBytes());
        scan.addFamily(family3.getBytes());
        scan.setMaxVersions(10);
        f = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("family_with_group")));
        scan.setFilter(f);
        scanner = multiCfHTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(keyValue.getFamily()),
                        Bytes.toString(keyValue.getQualifier()),
                        keyValue.getTimestamp(),
                        Bytes.toString(keyValue.getValue())
                );
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 6);
        scanner.close();
    }
}
