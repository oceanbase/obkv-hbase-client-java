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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.*;

public class OHTableMultiColumnFamilyTest {
    @Rule
    public ExpectedException  expectedException = ExpectedException.none();

    protected HTableInterface hTable;

    @Before
    public void before() throws Exception {
        hTable = ObHTableTestUtil.newOHTableClient("test_multi_cf");
        ((OHTableClient) hTable).init();
    }

    @After
    public void finish() throws IOException {
        hTable.close();
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
        Result result = hTable.get(get);
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
        result = hTable.get(get);
        Assert.assertEquals(3, result.raw().length);
        Assert.assertFalse(result.containsColumn(family2, family2_column1));
        Assert.assertFalse(result.containsColumn(family2, family2_column2));
        Assert.assertFalse(result.containsColumn(family3, family3_column1));

        get = new Get(toBytes("Key5"));
        result = hTable.get(get);
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
        ResultScanner scanner = hTable.getScanner(scan);
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
            result = hTable.get(get);
            if (!result.isEmpty()) {
                break;
            }
        }
        get = new Get(toBytes("Key2"));
        result = hTable.get(get);
        Assert.assertEquals(6 , result.raw().length);
        Assert.assertTrue(result.containsColumn(family1, family1_column1));
        Assert.assertTrue(result.containsColumn(family1, family1_column2));
        Assert.assertTrue(result.containsColumn(family1, family1_column3));
        Assert.assertTrue(result.containsColumn(family2, family2_column1));
        Assert.assertTrue(result.containsColumn(family3, family3_column1));
        Assert.assertTrue(result.containsColumn(family3, family3_column2));

        get = new Get(toBytes("Key3"));
        result = hTable.get(get);
        if (result.containsColumn(family1, family1_column1) || result.containsColumn(family2, family2_column1)) {
            mutator.flush();
        }
        get = new Get(toBytes("Key3"));
        result = hTable.get(get);
        Assert.assertEquals(2, result.raw().length);
        Assert.assertFalse(result.containsColumn(family1, family1_column1));
        Assert.assertFalse(result.containsColumn(family1, family1_column2));
        Assert.assertFalse(result.containsColumn(family1, family1_column3));
        Assert.assertFalse(result.containsColumn(family2, family2_column1));
        Assert.assertTrue(result.containsColumn(family3, family3_column1));
        Assert.assertTrue(result.containsColumn(family3, family3_column2));

        get = new Get(toBytes("Key9"));
        result = hTable.get(get);
        if (result.containsColumn(family1, family1_column1) || result.containsColumn(family2, family2_column1)) {
            mutator.flush();
        }
        get = new Get(toBytes("Key9"));
        result = hTable.get(get);
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
        scanner = hTable.getScanner(scan);
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
        hTable.batch(batchLsit);
        // f1c2 f1c3 f2c2 f3c1
        Get get = new Get(toBytes("Key1"));
        Result result = hTable.get(get);
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
        hTable.batch(batchLsit);
        get = new Get(toBytes("Key2"));
        result = hTable.get(get);
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
        hTable.batch(batchLsit);
        get = new Get(toBytes("Key3"));
        result = hTable.get(get);
        keyValues = result.raw();
        assertEquals(6, keyValues.length);

        batchLsit.clear();
        delete = new Delete(toBytes("Key4"));
        delete.deleteColumns(family1, family1_column2);
        delete.deleteColumns(family2, family2_column1);
        delete.deleteFamily(family3);
        batchLsit.add(delete);
        hTable.batch(batchLsit);
        get = new Get(toBytes("Key4"));
        get.setMaxVersions(10);
        result = hTable.get(get);
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
        hTable.batchCallback(batchLsit, new Batch.Callback<MutationResult>() {
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
            hTable.put(put);
        }
        hTable.flushCommits();

        Scan scan = new Scan();
        scan.setStartRow(toBytes("Key"));
        scan.setStopRow(toBytes("Kf"));
        ResultScanner scanner = hTable.getScanner(scan);
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
            hTable.append(append);
        }
        hTable.flushCommits();

        Scan scan = new Scan();
        scan.setStartRow(toBytes("Key"));
        scan.setStopRow(toBytes("Kf"));
        ResultScanner scanner = hTable.getScanner(scan);
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
            hTable.put(put);
        }

        Scan scan = new Scan();
        scan.addFamily(family1);
        scan.addFamily(family2);
        scan.setReversed(true);
        ResultScanner scanner2 = hTable.getScanner(scan);

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
            hTable.put(put);
        }

        Scan scan = new Scan();
        scan.setStartRow(toBytes("Key"));
        scan.setStopRow(toBytes("Kf"));
        scan.addColumn(family1, family1_column1);
        scan.addColumn(family2, family2_column1);
        ResultScanner scanner = hTable.getScanner(scan);

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
        scanner = hTable.getScanner(scan);

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

        scanner = hTable.getScanner(scan);

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

        scanner = hTable.getScanner(scan);

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
            hTable.put(put);
        }

        PrefixFilter filter = new PrefixFilter(toBytes("Key1"));
        Scan scan = new Scan();
        scan.setStartRow(toBytes("Key"));
        scan.setStopRow(toBytes("Kf"));
        scan.setFilter(filter);
        ResultScanner scanner = hTable.getScanner(scan);

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
            hTable.put(put);
        }
        hTable.flushCommits();

        // get with empty family
        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        Get get = new Get(toBytes("Key1"));
        Result result = hTable.get(get);
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
        Result result2 = hTable.get(get2);
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
        Result result3 = hTable.get(get3);
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
            hTable.delete(delete);
            put.add(family1, family1_column1, family1_value);
            put.add(family1, family1_column2, family1_value);
            put.add(family1, family1_column3, family1_value);
            put.add(family2, family2_column1, family2_value);
            put.add(family2, family2_column2, family2_value);
            put.add(family3, family3_column1, family3_value);
            hTable.put(put);
        }

        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        Delete delete = new Delete(toBytes("Key1"));
        delete.deleteColumns(family1, family1_column1);
        delete.deleteColumns(family2, family2_column1);
        hTable.delete(delete);
        // f1c2 f1c3 f2c2 f3c1
        Get get = new Get(toBytes("Key1"));
        Result result = hTable.get(get);
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
        hTable.delete(delete);
        get = new Get(toBytes("Key2"));
        result = hTable.get(get);
        keyValues = result.raw();
        assertEquals(1, keyValues.length);

        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        delete = new Delete(toBytes("Key3"));
        delete.deleteFamily(family1);
        delete.deleteColumns(family2, family2_column1);
        hTable.delete(delete);
        // f2c2 f3c1
        get = new Get(toBytes("Key3"));
        result = hTable.get(get);
        keyValues = result.raw();
        assertEquals(2, keyValues.length);

        // f1c1 f1c2 f1c3 f2c1 f2c2 f3c1
        delete = new Delete(toBytes("Key4"));
        hTable.delete(delete);
        // null
        get = new Get(toBytes("Key4"));
        result = hTable.get(get);
        keyValues = result.raw();
        assertEquals(0, keyValues.length);

        // f1c1 f2c1 f2c2
        delete = new Delete(toBytes("Key5"));
        delete.deleteColumns(family1, family1_column2);
        delete.deleteColumns(family1, family1_column3);
        delete.deleteColumns(family3, family3_column1);
        hTable.delete(delete);
        // null
        get = new Get(toBytes("Key5"));
        result = hTable.get(get);
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
            hTable.put(put);
        }

        delete = new Delete(toBytes("Key6"));
        delete.deleteColumn(family1, family1_column2);
        delete.deleteColumn(family2, family2_column1);
        hTable.delete(delete);
        get = new Get(toBytes("Key6"));
        result = hTable.get(get);
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

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);

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

        hTable.put(putKey1Fam1Column1MinTs);
        hTable.put(putKey3Fam1Column1Ts1);
        hTable.put(putKey1Fam1Column2MinTs);
        hTable.put(putKey1Fam1Column2Ts3);
        hTable.put(putKey2Fam1Column2Ts3);
        hTable.put(putKey2Fam1Column3Ts1);
        hTable.put(putKey3Fam1Column3Ts1);
        hTable.put(putKey3Fam1Column2Ts6);
        hTable.put(putKey2Fam1Column3Ts6);

        // test DeleteFamilyVersion single cf
        get = new Get(toBytes(key1));
        get.addFamily(family1);
        get.setTimeStamp(minTimeStamp);
        get.setMaxVersions(10);
        Result r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        get = new Get(toBytes(key3));
        get.addFamily(family1);
        get.setTimeStamp(timeStamp1);
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        get = new Get(toBytes(key2));
        get.addFamily(family1);
        get.setTimeStamp(timeStamp3);
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(2, r.raw().length);

        Delete delKey1MinTs = new Delete(toBytes(key1));
        delKey1MinTs.deleteFamilyVersion(family1, minTimeStamp);
        hTable.delete(delKey1MinTs);

        get = new Get(toBytes(key1));
        get.addFamily(family1);
        get.setTimeStamp(minTimeStamp);
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        Delete delKey3Ts1 = new Delete(toBytes(key3));
        delKey3Ts1.deleteFamilyVersion(family1, timeStamp1);
        hTable.delete(delKey3Ts1);

        get = new Get(toBytes(key3));
        get.addFamily(family1);
        get.setTimeStamp(timeStamp1);
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        Delete delKey2Ts3 = new Delete(toBytes(key2));
        delKey2Ts3.deleteFamilyVersion(family1, timeStamp3);
        hTable.delete(delKey2Ts3);

        get = new Get(toBytes(key2));
        get.addFamily(family1);
        get.setTimeStamp(timeStamp3);
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);

        Scan scan = new Scan();
        scan.setStartRow(toBytes(key1));
        scan.setStopRow("scanKey4x".getBytes());
        scan.addFamily(family1);
        scan.setMaxVersions(10);
        ResultScanner scanner = hTable.getScanner(scan);
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

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);

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

        hTable.put(putKey1Fam1Column3Ts6);
        hTable.put(putKey1Fam2Column2Ts2);
        hTable.put(putKey1Fam2Column3Ts2);
        hTable.put(putKey1Fam1Column2Ts1);
        hTable.put(putKey2Fam1Column2Ts8);
        hTable.put(putKey2Fam2Column3Ts1);
        hTable.put(putKey2Fam1Column1Ts1);
        hTable.put(putKey2Fam2Column1Ts3);
        hTable.put(putKey3Fam1Column2Ts9);
        hTable.put(putKey3Fam2Column3Ts10);
        hTable.put(putKey3Fam2Column1Ts10);
        hTable.put(putKey3Fam1Column2Ts2);

        Get getKey1 = new Get(toBytes(key1));
        getKey1.addFamily(family1);
        getKey1.addFamily(family2);
        getKey1.setMaxVersions(10);
        r = hTable.get(getKey1);
        Assert.assertEquals(4, r.raw().length);

        Get getKey2 = new Get(toBytes(key2));
        getKey2.addFamily(family1);
        getKey2.addFamily(family2);
        getKey2.setMaxVersions(10);
        r = hTable.get(getKey2);
        Assert.assertEquals(4, r.raw().length);

        Get getKey3 = new Get(toBytes(key3));
        getKey3.addFamily(family1);
        getKey3.addFamily(family2);
        getKey3.setMaxVersions(10);
        r = hTable.get(getKey3);
        Assert.assertEquals(4, r.raw().length);

        Delete delKey1Ts_6_2 = new Delete(toBytes(key1));
        delKey1Ts_6_2.deleteFamilyVersion(family1, timeStamp6);
        delKey1Ts_6_2.deleteFamilyVersion(family2, timeStamp2);
        hTable.delete(delKey1Ts_6_2);

        getKey1 = new Get(toBytes(key1));
        getKey1.addFamily(family1);
        getKey1.addFamily(family2);
        getKey1.setMaxVersions(10);
        r = hTable.get(getKey1);
        Assert.assertEquals(1, r.raw().length);
        for (KeyValue kv : r.raw()) {
            Assert.assertEquals(timeStamp1, kv.getTimestamp());
        }

        Delete delKey2Ts_8_3 = new Delete(toBytes(key2));
        delKey2Ts_8_3.deleteFamilyVersion(family1, timeStamp8);
        delKey2Ts_8_3.deleteFamilyVersion(family2, timeStamp3);
        hTable.delete(delKey2Ts_8_3);

        getKey2 = new Get(toBytes(key2));
        getKey2.addFamily(family1);
        getKey2.addFamily(family2);
        getKey2.setMaxVersions(10);
        r = hTable.get(getKey2);
        Assert.assertEquals(0, r.raw().length);

        Delete delKey3Ts_2_10 = new Delete(toBytes(key3));
        delKey3Ts_2_10.deleteFamilyVersion(family1, timeStamp2);
        delKey3Ts_2_10.deleteFamilyVersion(family2, timeStamp10);
        hTable.delete(delKey3Ts_2_10);

        getKey3 = new Get(toBytes(key3));
        getKey3.addFamily(family1);
        getKey3.addFamily(family2);
        getKey3.setMaxVersions(10);
        r = hTable.get(getKey3);
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
        scanner = hTable.getScanner(scan);
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

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
    }
}
