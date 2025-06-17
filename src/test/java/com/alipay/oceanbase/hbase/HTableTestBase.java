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
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ALL;
import static org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ONE;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.*;

public abstract class HTableTestBase extends HTableMultiCFTestBase {

    @Rule
    public ExpectedException     expectedException = ExpectedException.none();

    protected static Table       hTable;
    private static AtomicInteger count             = new AtomicInteger(0);
    private static AtomicInteger idx               = new AtomicInteger(0);

    @Test
    public void testTableGroup() throws IOError, IOException {
        String key = "putKey";
        String column1 = "putColumn1";
        String value = "value333444";
        long timestamp = System.currentTimeMillis();
        // put data
        Put put = new Put(toBytes(key));
        KeyValue kv = new KeyValue(toBytes(key), "family_group".getBytes(), column1.getBytes(),
            timestamp, toBytes(value + "1"));
        put.add(kv);
        hTable.put(put);
        // test get with empty family
        Get get = new Get(toBytes(key));
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        for (Cell keyValue : r.rawCells()) {
            Assert.assertEquals(key, Bytes.toString(CellUtil.cloneRow(keyValue)));
            Assert.assertEquals(column1, Bytes.toString(CellUtil.cloneQualifier(keyValue)));
            Assert.assertEquals(timestamp, keyValue.getTimestamp());
            Assert.assertEquals(value + "1", Bytes.toString(CellUtil.cloneValue(keyValue)));
            System.out.println("rowKey: " + new String(CellUtil.cloneRow(keyValue)) + " family :"
                               + new String(CellUtil.cloneFamily(keyValue)) + " columnQualifier:"
                               + new String(CellUtil.cloneQualifier(keyValue)) + " timestamp:"
                               + keyValue.getTimestamp() + " value:"
                               + new String(CellUtil.cloneValue(keyValue)));
        }

        get = new Get(toBytes(key));
        get.setTimeStamp(r.rawCells()[0].getTimestamp());
        get.setMaxVersions(1);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        for (Cell keyValue : r.rawCells()) {
            Assert.assertEquals(key, Bytes.toString(CellUtil.cloneRow(keyValue)));
            Assert.assertEquals(column1, Bytes.toString(CellUtil.cloneQualifier(keyValue)));
            Assert.assertEquals(timestamp, keyValue.getTimestamp());
            Assert.assertEquals(value + "1", Bytes.toString(CellUtil.cloneValue(keyValue)));
            System.out.println("rowKey: " + new String(CellUtil.cloneRow(keyValue)) + " family :"
                               + new String(CellUtil.cloneFamily(keyValue)) + " columnQualifier:"
                               + new String(CellUtil.cloneQualifier(keyValue)) + " timestamp:"
                               + keyValue.getTimestamp() + " value:"
                               + new String(CellUtil.cloneValue(keyValue)));
        }

        // test scan with empty family
        Scan scan = new Scan(toBytes(key));
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                Assert.assertEquals(key, Bytes.toString(CellUtil.cloneRow(keyValue)));
                Assert.assertEquals(column1, Bytes.toString(CellUtil.cloneQualifier(keyValue)));
                Assert.assertEquals(timestamp, keyValue.getTimestamp());
                Assert.assertEquals(value + "1", Bytes.toString(CellUtil.cloneValue(keyValue)));
                System.out.println("rowKey: " + new String(CellUtil.cloneRow(keyValue))
                                   + " family :" + new String(CellUtil.cloneFamily(keyValue))
                                   + " columnQualifier:"
                                   + new String(CellUtil.cloneQualifier(keyValue)) + " timestamp:"
                                   + keyValue.getTimestamp() + " value:"
                                   + new String(CellUtil.cloneValue(keyValue)));
            }
        }

        // test getScanners with non-partition and empty family
        List<ResultScanner> scanners = null;
        if (hTable instanceof OHTableClient) {
            scanners = ((OHTableClient) hTable).getScanners(scan);
        } else if (hTable instanceof OHTable) {
            scanners = ((OHTable) hTable).getScanners(scan);
        } else if (hTable instanceof OHTablePool.PooledOHTable) {
            scanners = ((OHTablePool.PooledOHTable) hTable).getScanners(scan);
        } else {
            throw new IllegalArgumentException(
                "just support for OHTable, OHTableClient and PooledOHTable");
        }
        Assert.assertNotNull(scanners);
        Assert.assertTrue(!scanners.isEmpty());
        Assert.assertEquals(1, scanners.size());
        for (ResultScanner singlePartScanner : scanners) {
            for (Result result : singlePartScanner) {
                for (Cell keyValue : result.rawCells()) {
                    Assert.assertEquals(key, Bytes.toString(CellUtil.cloneRow(keyValue)));
                    Assert.assertEquals(column1, Bytes.toString(CellUtil.cloneQualifier(keyValue)));
                    Assert.assertEquals(timestamp, keyValue.getTimestamp());
                    Assert.assertEquals(value + "1", Bytes.toString(CellUtil.cloneValue(keyValue)));
                    System.out.println("rowKey: " + new String(CellUtil.cloneRow(keyValue))
                                       + " family :" + new String(CellUtil.cloneFamily(keyValue))
                                       + " columnQualifier:"
                                       + new String(CellUtil.cloneQualifier(keyValue))
                                       + " timestamp:" + keyValue.getTimestamp() + " value:"
                                       + new String(CellUtil.cloneValue(keyValue)));
                }
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

        Put put = new Put(toBytes(key));
        KeyValue kv = new KeyValue(toBytes(key), family.getBytes(), column1.getBytes(), timestamp,
            toBytes(value));
        put.add(kv);
        hTable.put(put);
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), toBytes(column1));
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        for (Cell keyValue : r.rawCells()) {
            System.out.println("rowKey: " + new String(CellUtil.cloneRow(keyValue))
                               + " columnQualifier:"
                               + new String(CellUtil.cloneQualifier(keyValue)) + " timestamp:"
                               + keyValue.getTimestamp() + " value:"
                               + new String(CellUtil.cloneValue(keyValue)));
            Assert.assertEquals(key, Bytes.toString(CellUtil.cloneRow(keyValue)));
            Assert.assertEquals(column1, Bytes.toString(CellUtil.cloneQualifier(keyValue)));
            Assert.assertEquals(timestamp, keyValue.getTimestamp());
            Assert.assertEquals(value, Bytes.toString(CellUtil.cloneValue(keyValue)));
        }

        put = new Put(toBytes(key));
        kv = new KeyValue(toBytes(key), family.getBytes(), column1.getBytes(), timestamp + 1,
            toBytes(value));
        put.add(kv);
        hTable.put(put);
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), toBytes(column1));
        get.setMaxVersions(2);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        get.setMaxVersions(1);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        Delete delete = new Delete(key.getBytes());
        delete.addFamily(family.getBytes());
        hTable.delete(delete);

        for (Cell keyValue : r.rawCells()) {
            System.out.println("rowKey: " + new String(CellUtil.cloneRow(keyValue))
                               + " columnQualifier:"
                               + new String(CellUtil.cloneQualifier(keyValue)) + " timestamp:"
                               + keyValue.getTimestamp() + " value:"
                               + new String(CellUtil.cloneValue(keyValue)));
            Assert.assertEquals(key, Bytes.toString(CellUtil.cloneRow(keyValue)));
            Assert.assertEquals(column1, Bytes.toString(CellUtil.cloneQualifier(keyValue)));
            Assert.assertEquals(timestamp + 1, keyValue.getTimestamp());
            Assert.assertEquals(value, Bytes.toString(CellUtil.cloneValue(keyValue)));
        }

        try {
            for (int j = 0; j < 10; j++) {
                put = new Put((key + "_" + j).getBytes());
                put.addColumn(family.getBytes(), column1.getBytes(), timestamp + 2, toBytes(value));
                put.addColumn(family.getBytes(), column2.getBytes(), timestamp + 2, toBytes(value));
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
                for (Cell keyValue : result.rawCells()) {
                    System.out.println("rowKey: " + new String(CellUtil.cloneRow(keyValue))
                                       + " columnQualifier:"
                                       + new String(CellUtil.cloneQualifier(keyValue))
                                       + " timestamp:" + keyValue.getTimestamp() + " value:"
                                       + new String(CellUtil.cloneValue(keyValue)));
                    Assert.assertEquals(key + "_" + i, Bytes.toString(CellUtil.cloneRow(keyValue)));
                    Assert.assertTrue(column1.equals(Bytes.toString(CellUtil
                        .cloneQualifier(keyValue)))
                                      || column2.equals(Bytes.toString(CellUtil
                                          .cloneQualifier(keyValue))));
                    Assert.assertEquals(timestamp + 2, keyValue.getTimestamp());
                    Assert.assertEquals(value, Bytes.toString(CellUtil.cloneValue(keyValue)));
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
                for (Cell keyValue : result.rawCells()) {
                    System.out.println("rowKey: " + new String(CellUtil.cloneRow(keyValue))
                                       + " columnQualifier:"
                                       + new String(CellUtil.cloneQualifier(keyValue))
                                       + " timestamp:" + keyValue.getTimestamp() + " value:"
                                       + new String(CellUtil.cloneValue(keyValue)));
                    Assert.assertEquals(key + "_" + i, Bytes.toString(CellUtil.cloneRow(keyValue)));
                    Assert.assertTrue(column1.equals(Bytes.toString(CellUtil
                        .cloneQualifier(keyValue)))
                                      || column2.equals(Bytes.toString(CellUtil
                                          .cloneQualifier(keyValue))));
                    Assert.assertEquals(value, Bytes.toString(CellUtil.cloneValue(keyValue)));
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
                delete.addFamily(family.getBytes());
                hTable.delete(delete);
            }
        }

    }

    /*
    * CREATE TABLE `test$partitionFamily1` (
        `K` varbinary(1024) NOT NULL,
        `Q` varbinary(256) NOT NULL,
        `T` bigint(20) NOT NULL,
        `V` varbinary(1024) DEFAULT NULL,
        PRIMARY KEY (`K`, `Q`, `T`)
    ) partition by key(`K`) partitions 17;
    * */
    @Test
    public void testKeyPartWithGetScanners() throws Exception {
        testKeyPartGetScannersOneThread("partitionFamily1");
        testKeyPartGetScannersMultiThread("partitionFamily1");
    }

    private void testKeyPartGetScannersOneThread(String family) throws Exception {
        String key = "putKey";
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        String value = "value";
        long timestamp = System.currentTimeMillis();
        Delete delete = new Delete(key.getBytes());
        delete.addFamily(family.getBytes());
        hTable.delete(delete);

        Put put = null;

        try {
            Set<String> keys = new HashSet<>();
            for (int j = 0; j < 100; j++) {
                String new_key = key + "_" + j;
                keys.add(new_key);
                put = new Put(new_key.getBytes());
                put.addColumn(family.getBytes(), column1.getBytes(), timestamp + 2, toBytes(value));
                put.addColumn(family.getBytes(), column2.getBytes(), timestamp + 2, toBytes(value));
                hTable.put(put);
            }

            Scan scan = new Scan();
            scan.addColumn(family.getBytes(), column1.getBytes());
            scan.addColumn(family.getBytes(), column2.getBytes());

            List<ResultScanner> scanners = null;
            if (hTable instanceof OHTableClient) {
                scanners = ((OHTableClient) hTable).getScanners(scan);
            } else if (hTable instanceof OHTable) {
                scanners = ((OHTable) hTable).getScanners(scan);
            } else if (hTable instanceof OHTablePool.PooledOHTable) {
                scanners = ((OHTablePool.PooledOHTable) hTable).getScanners(scan);
            }
            else {
                throw new IllegalArgumentException("just support for OHTable, OHTableClient and PooledOHTable");
            }
            Assert.assertEquals(17, scanners.size());

            int count  = 0;
            int idx = 0;
            for (ResultScanner scanner : scanners) {
                for (Result result : scanner) {
                    boolean countAdd = true;
                    for (Cell keyValue : result.rawCells()) {
                        System.out.println("Partition No." + idx + ": rowKey: " + Bytes.toString(CellUtil.cloneRow(keyValue))
                                + " columnQualifier:" + Bytes.toString(CellUtil.cloneQualifier(keyValue))
                                + " timestamp:" + keyValue.getTimestamp() + " value:"
                                + Bytes.toString(CellUtil.cloneValue(keyValue)));
                        Assert.assertTrue(keys.contains(Bytes.toString(CellUtil.cloneRow(keyValue))));
                        Assert.assertTrue(column1.equals(Bytes.toString(CellUtil.cloneQualifier(keyValue)))
                                || column2.equals(Bytes.toString(CellUtil.cloneQualifier(keyValue))));
                        Assert.assertEquals(timestamp + 2, keyValue.getTimestamp());
                        Assert.assertEquals(value, Bytes.toString(CellUtil.cloneValue(keyValue)));
                        if (countAdd) {
                            countAdd = false;
                            count++;
                        }
                    }
                }
                idx++;
            }
            Assert.assertEquals(100, count);
        } finally {
            for (int j = 0; j < 100; j++) {
                delete = new Delete(toBytes(key + "_" + j));
                delete.addFamily(family.getBytes());
                hTable.delete(delete);
            }
        }
    }

    private void testKeyPartGetScannersMultiThread(String family) throws Exception {
        String key = "putKey";
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        String value = "value";
        long timestamp = System.currentTimeMillis();
        Delete delete = new Delete(key.getBytes());
        delete.addFamily(family.getBytes());
        hTable.delete(delete);

        Put put = null;

        try {
            Set<String> keys = new HashSet<>();
            for (int j = 0; j < 100; j++) {
                String new_key = key + "_" + j;
                keys.add(new_key);
                put = new Put(new_key.getBytes());
                put.addColumn(family.getBytes(), column1.getBytes(), timestamp + 2, toBytes(value));
                put.addColumn(family.getBytes(), column2.getBytes(), timestamp + 2, toBytes(value));
                hTable.put(put);
            }

            Scan scan = new Scan();
            scan.addColumn(family.getBytes(), column1.getBytes());
            scan.addColumn(family.getBytes(), column2.getBytes());

            List<ResultScanner> scanners = null;
            if (hTable instanceof OHTableClient) {
                scanners = ((OHTableClient) hTable).getScanners(scan);
            } else if (hTable instanceof OHTable) {
                scanners = ((OHTable) hTable).getScanners(scan);
            } else if (hTable instanceof OHTablePool.PooledOHTable) {
                scanners = ((OHTablePool.PooledOHTable) hTable).getScanners(scan);
            } else {
                throw new IllegalArgumentException("just support for OHTable, OHTableClient and PooledOHTable");
            }
            // scanners' size is the number of partitions
            Assert.assertEquals(17, scanners.size());

            ExecutorService executorService = Executors.newFixedThreadPool(scanners.size());

            for (ResultScanner scanner : scanners) {
                executorService.submit(() -> {
                    int localIdx = idx.getAndIncrement();
                    for (Result result : scanner) {
                        boolean countAdd = true;
                        for (Cell keyValue : result.rawCells()) {
                            System.out.println("Partition No." + localIdx + ": rowKey: " + Bytes.toString(CellUtil.cloneRow(keyValue))
                                    + " columnQualifier:" + Bytes.toString(CellUtil.cloneQualifier(keyValue))
                                    + " timestamp:" + keyValue.getTimestamp() + " value:"
                                    + Bytes.toString(CellUtil.cloneValue(keyValue)));
                            Assert.assertTrue(keys.contains(Bytes.toString(CellUtil.cloneRow(keyValue))));
                            Assert.assertTrue(column1.equals(Bytes.toString(CellUtil.cloneQualifier(keyValue)))
                                    || column2.equals(Bytes.toString(CellUtil.cloneQualifier(keyValue))));
                            Assert.assertEquals(timestamp + 2, keyValue.getTimestamp());
                            Assert.assertEquals(value, Bytes.toString(CellUtil.cloneValue(keyValue)));
                            if (countAdd) {
                                countAdd = false;
                                count.incrementAndGet();
                            }
                        }
                    }
                });
            }

            executorService.shutdown();
            try {
                // wait for all tasks done
                if (!executorService.awaitTermination(500L, TimeUnit.MILLISECONDS)) {
                    executorService.shutdownNow();
                    if (!executorService.awaitTermination(500L, TimeUnit.MILLISECONDS)) {
                        System.err.println("the thread pool did not shut down");
                    }
                }
            } catch (InterruptedException ie) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }

            Assert.assertEquals(100, count.get());
            idx = new AtomicInteger(0);
            count = new AtomicInteger(0);
        } finally {
            for (int j = 0; j < 100; j++) {
                delete = new Delete(toBytes(key + "_" + j));
                delete.addFamily(family.getBytes());
                hTable.delete(delete);
            }
        }
    }

    /*
    * CREATE TABLE `test$familyRange` (
        `K` varbinary(1024) NOT NULL,
        `Q` varbinary(256) NOT NULL,
        `T` bigint(20) NOT NULL,
        `V` varbinary(1024) DEFAULT NULL,
        PRIMARY KEY (`K`, `Q`, `T`)
    ) partition by range columns (`K`) (
        PARTITION p0 VALUES LESS THAN ('d'),
        PARTITION p1 VALUES LESS THAN ('j'),
        PARTITION p2 VALUES LESS THAN MAXVALUE
    );
    * */
    @Test
    public void testRangePartWithGetScanners() throws Exception {
        testRangePartGetScannersMultiThread("familyRange");
    }

    private void testRangePartGetScannersMultiThread(String family) throws Exception {
        String key = null;
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        String value = "value";
        long timestamp = System.currentTimeMillis();

        Set<String> keys = new HashSet<>();
        Put put = null;

        try {
            for (int j = 0; j < 26; j++) {
                key = generateRandomStringByUUID(5);
                keys.add(key);
                put = new Put(key.getBytes());
                put.addColumn(family.getBytes(), column1.getBytes(), timestamp + 2, toBytes(value));
                put.addColumn(family.getBytes(), column2.getBytes(), timestamp + 2, toBytes(value));
                hTable.put(put);
            }

            Scan scan = new Scan();
            scan.addColumn(family.getBytes(), column1.getBytes());
            scan.addColumn(family.getBytes(), column2.getBytes());

            List<ResultScanner> scanners = null;
            if (hTable instanceof OHTableClient) {
                scanners = ((OHTableClient) hTable).getScanners(scan);
            } else if (hTable instanceof OHTable) {
                scanners = ((OHTable) hTable).getScanners(scan);
            } else if (hTable instanceof OHTablePool.PooledOHTable) {
                scanners = ((OHTablePool.PooledOHTable) hTable).getScanners(scan);
            } else {
                throw new IllegalArgumentException("just support for OHTable, OHTableClient and PooledOHTable");
            }
            // scanners' size is the number of partitions
            Assert.assertEquals(3, scanners.size());

            ExecutorService executorService = Executors.newFixedThreadPool(scanners.size());

            for (ResultScanner scanner : scanners) {
                executorService.submit(() -> {
                    int localIdx = idx.getAndIncrement();
                    for (Result result : scanner) {
                        boolean countAdd = true;
                        for (Cell keyValue : result.rawCells()) {
                            System.out.println("Partition No." + localIdx + ": rowKey: " + Bytes.toString(CellUtil.cloneRow(keyValue))
                                    + " columnQualifier:" + Bytes.toString(CellUtil.cloneQualifier(keyValue))
                                    + " timestamp:" + keyValue.getTimestamp() + " value:"
                                    + Bytes.toString(CellUtil.cloneValue(keyValue)));
                            // 假设 keys, column1, column2, timestamp 和 value 是有效的变量
                            Assert.assertTrue(keys.contains(Bytes.toString(CellUtil.cloneRow(keyValue))));
                            Assert.assertTrue(column1.equals(Bytes.toString(CellUtil.cloneQualifier(keyValue)))
                                    || column2.equals(Bytes.toString(CellUtil.cloneQualifier(keyValue))));
                            Assert.assertEquals(timestamp + 2, keyValue.getTimestamp());
                            Assert.assertEquals(value, Bytes.toString(CellUtil.cloneValue(keyValue)));
                            if (countAdd) {
                                countAdd = false;
                                count.incrementAndGet();
                            }
                        }
                    }
                });
            }

            executorService.shutdown();
            try {
                // wait for all tasks done
                if (!executorService.awaitTermination(500L, TimeUnit.MILLISECONDS)) {
                    executorService.shutdownNow();
                    if (!executorService.awaitTermination(500L, TimeUnit.MILLISECONDS)) {
                        System.err.println("the thread pool did not shut down");
                    }
                }
            } catch (InterruptedException ie) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }

            Assert.assertEquals(26, count.get());
            idx = new AtomicInteger(0);
            count = new AtomicInteger(0);
        } finally {
            Object[] keyList = keys.toArray();
            for (int j = 0; j < keyList.length; j++) {
                key = (String) keyList[j];
                Delete delete = new Delete(toBytes(key));
                delete.addFamily(family.getBytes());
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
        long startTimeBase = 1539700745718L;
        for (int j = 1; j <= testNum; j++) {
            byte[] rowkey = new byte[keyBytes.length + 8];
            long current = startTimeBase + j * 10;
            // System.out.println(current);
            byte[] currentBytes = Bytes.toBytes(current);
            System.arraycopy(keyBytes, 0, rowkey, 0, keyBytes.length);
            System.arraycopy(currentBytes, 0, rowkey, keyBytes.length, currentBytes.length);
            Put put = new Put(rowkey);
            KeyValue kv = new KeyValue(rowkey, "family1".getBytes(), column1.getBytes(),
                toBytes(value));
            put.add(kv);
            kv = new KeyValue(rowkey, "family1".getBytes(), column2.getBytes(), toBytes(value));
            put.add(kv);
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
            for (Cell keyValue : result.rawCells()) {
                byte[] rowkey = CellUtil.cloneRow(keyValue);
                byte[] readKey = new byte[keyBytes.length];
                System.arraycopy(rowkey, 0, readKey, 0, keyBytes.length);
                byte[] timestamp = new byte[8];
                System.arraycopy(rowkey, keyBytes.length, timestamp, 0, 8);
                // System.out.println("key :" + Bytes.toString(readKey) + Bytes.toLong(timestamp)
                //                    + " column :" + Bytes.toString(CellUtil.cloneQualifier(keyValue)));
            }
            i++;
        }
        System.out.println("count " + i);
        assertEquals(testNum - 100 + 1, i);
    }

    @Test
    public void testMultiPut() throws IOException {
        String column1 = "column1";
        String column2 = "column2";
        String value1 = "value1";
        String family = "family1";
        Put put1 = new Put(Bytes.toBytes("testKey"));
        KeyValue kv = new KeyValue(Bytes.toBytes("testKey"), toBytes(family), toBytes(column2), toBytes(value1));
        put1.add(kv);
        kv = new KeyValue(Bytes.toBytes("testKey"), toBytes(family), toBytes(column2), System.currentTimeMillis(), toBytes(value1));
        put1.add(kv);

        Put put2 = new Put(Bytes.toBytes("testKey1"));
        kv = new KeyValue(Bytes.toBytes("testKey1"), toBytes(family), toBytes(column2), toBytes(value1));
        put2.add(kv);
        kv = new KeyValue(Bytes.toBytes("testKey1"), toBytes(family), toBytes(column2), System.currentTimeMillis(), toBytes(value1));
        put2.add(kv);
        List<Put> puts = new ArrayList<>();
        puts.add(put1);
        puts.add(put2);
        hTable.put(puts);

        // put same k, q, t
        Put put3 = new Put(Bytes.toBytes("testKey"));
        kv = new KeyValue(Bytes.toBytes("testKey"), toBytes(family), toBytes(column1), 0L, toBytes(value1));
        put3.add(kv);
        kv = new KeyValue(Bytes.toBytes("testKey"), toBytes(family), toBytes(column1), 0L, toBytes(value1));
        put3.add(kv);

        Put put4 = new Put(Bytes.toBytes("testKey"));
        kv = new KeyValue(Bytes.toBytes("testKey"), toBytes(family), toBytes(column1), System.currentTimeMillis(), toBytes(value1));
        put4.add(kv);
        kv = new KeyValue(Bytes.toBytes("testKey"), toBytes(family), toBytes(column1), System.currentTimeMillis(), toBytes(value1));
        put4.add(kv);
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
                put.addColumn(toBytes(family), toBytes(column1), toBytes(value));
                put.addColumn(toBytes(family), toBytes(column2), System.currentTimeMillis(),
                    toBytes(value));
                puts.add(put);
            }

            for (String key : keys) {
                // put same k, q, t
                Put put = new Put(Bytes.toBytes(key));
                put.addColumn(toBytes(family), toBytes(column3), 100L, toBytes(value));
                put.addColumn(toBytes(family), toBytes(column3), 100L, toBytes(value));
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
            assertEquals(res[0].rawCells().length, 3);
        }
    }

    @Ignore
    public void testMultiPartitionDel() throws IOException {
        String[] keys = new String[] { "putKey1", "putKey2", "putKey3", "putKey4", "putKey5",
                "putKey6", "putKey7", "putKey8", "putKey9", "putKey10" };

        String column1 = "column1";
        String column2 = "column2";
        String column3 = "column3";
        String family = "familyPartition";
        // delete
        {
            List<Delete> deletes = new ArrayList<Delete>();
            for (String key : keys) {
                Delete del = new Delete(Bytes.toBytes(key));
                del.addColumns(toBytes(family), toBytes(column1));
                del.addColumns(toBytes(family), toBytes(column2), System.currentTimeMillis());
                deletes.add(del);
            }

            for (String key : keys) {
                // del same k, q, t
                Delete del = new Delete(Bytes.toBytes(key));
                del.addColumn(toBytes(family), toBytes(column3), 100L);
                del.addColumn(toBytes(family), toBytes(column3), 100L);
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
                assertEquals(res[i].rawCells().length, 0);
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
        String family = "family1";
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.addFamily(toBytes(family));

        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Get get;
        Result r;
        ColumnPrefixFilter filter;

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
        Assert.assertEquals(7, r.rawCells().length);

        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter;
        singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes(family),
            Bytes.toBytes(column1), CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value1)));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueExcludeFilter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        DependentColumnFilter dependentColumnFilter = new DependentColumnFilter(
            Bytes.toBytes(family), Bytes.toBytes(column1), false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(dependentColumnFilter);
        r = hTable.get(get);
        Assert.assertEquals(3, r.rawCells().length);

        dependentColumnFilter = new DependentColumnFilter(Bytes.toBytes(family),
            Bytes.toBytes(column1), true);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(dependentColumnFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        dependentColumnFilter = new DependentColumnFilter(Bytes.toBytes(family),
            Bytes.toBytes(column1), false, CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value2)));
        get = new Get(toBytes(key2));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(dependentColumnFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        dependentColumnFilter = new DependentColumnFilter(Bytes.toBytes(family),
            Bytes.toBytes(column2), false, CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value2)));
        get = new Get(toBytes(key2));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(dependentColumnFilter);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        filter = new ColumnPrefixFilter(Bytes.toBytes("e"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        filter = new ColumnPrefixFilter(Bytes.toBytes("a"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filter);
        r = hTable.get(get);
        Assert.assertEquals(3, r.rawCells().length);

        filter = new ColumnPrefixFilter(Bytes.toBytes("d"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        filter = new ColumnPrefixFilter(Bytes.toBytes("b"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        FilterList filterList = new FilterList(MUST_PASS_ONE);
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("a")));
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("d")));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(7, r.rawCells().length);

        filterList = new FilterList(MUST_PASS_ALL);
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("a")));
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("d")));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        filterList = new FilterList(MUST_PASS_ONE);
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("c")));
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("d")));
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("e")));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        filterList = new FilterList(MUST_PASS_ALL);
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("d")));
        filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("de")));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        ColumnPaginationFilter f = new ColumnPaginationFilter(8, 0);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(f);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        Put putKey1Column3Value1 = new Put(toBytes(key1));
        putKey1Column3Value1.addColumn(toBytes(family), toBytes("ggg"), toBytes(value1));
        tryPut(hTable, putKey1Column3Value1);

        f = new ColumnPaginationFilter(8, 0);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(f);
        r = hTable.get(get);
        Assert.assertEquals(3, r.rawCells().length);

        f = new ColumnPaginationFilter(8, Bytes.toBytes("abc"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(f);
        r = hTable.get(get);
        Assert.assertEquals(3, r.rawCells().length);

        f = new ColumnPaginationFilter(8, Bytes.toBytes("bc"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(f);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        f = new ColumnPaginationFilter(8, Bytes.toBytes("ef"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(f);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        f = new ColumnPaginationFilter(8, Bytes.toBytes("h"));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(f);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        Scan scan;
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        ResultScanner scanner = hTable.getScanner(scan);

        int res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                if (res_count < 8) {
                    Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(keyValue));
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
            for (Cell keyValue : result.rawCells()) {
                if (res_count < 4) {
                    Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(keyValue));
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
            for (Cell keyValue : result.rawCells()) {
                if (res_count < 5) {
                    Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(keyValue));
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
            for (Cell keyValue : result.rawCells()) {
                if (res_count < 2) {
                    Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(keyValue));
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
            for (Cell keyValue : result.rawCells()) {
                if (res_count < 1) {
                    Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(keyValue));
                }
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 1);
        scanner.close();

        Delete deleteKey3Family = new Delete(toBytes("getKey3"));
        deleteKey3Family.addFamily(toBytes(family));
        Delete deleteKey4Family = new Delete(toBytes("getKey4"));
        deleteKey4Family.addFamily(toBytes(family));
        hTable.delete(deleteKey3Family);
        hTable.delete(deleteKey4Family);

        Put putKey3Column3Value1 = new Put(toBytes("getKey3"));
        putKey3Column3Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));
        tryPut(hTable, putKey3Column3Value1);
        Put putKey4Column3Value1 = new Put(toBytes("getKey4"));
        putKey4Column3Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));
        tryPut(hTable, putKey4Column3Value1);

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey5".getBytes());
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
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
            for (Cell keyValue : result.rawCells()) {
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
            for (Cell keyValue : result.rawCells()) {
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 12);
        scanner.close();

        long timestamp = System.currentTimeMillis();
        putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), timestamp,
            toBytes(value1));

        putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), timestamp,
            toBytes(value1));

        putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

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
            Bytes.toBytes(column1), false, CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value1)));
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("getKey1".getBytes());
        scan.setStopRow("getKey3".getBytes());
        scan.setMaxVersions(10);
        scan.setFilter(dependentColumnFilter);
        scanner = hTable.getScanner(scan);

        long prevTimestamp = -1;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
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
    public void testRowRangeFilter() throws Exception {
        String key1 = "ka1";
        String key2 = "kb2";
        String column1 = "abc";
        String column2 = "def";
        String value1 = "value1";
        String value2 = "value2";
        String family = "family1";

        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.addFamily(toBytes(family));

        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);

        Scan scan;
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>();
        ranges.add(new MultiRowRangeFilter.RowRange(Bytes.toBytes("ka3"), true, Bytes.toBytes("kd4"), false));
        ranges.add(new MultiRowRangeFilter.RowRange(Bytes.toBytes("c"), true, Bytes.toBytes("d$%%"), false));
        MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
        scan.setFilter(filter);
        ResultScanner scanner = hTable.getScanner(scan);

        int res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(CellUtil.cloneRow(keyValue)),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 2);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setReversed(true);
        ranges = new ArrayList<>();
        ranges.add(new MultiRowRangeFilter.RowRange(Bytes.toBytes("ka3"), true, Bytes.toBytes("kd4"), false));
        ranges.add(new MultiRowRangeFilter.RowRange(Bytes.toBytes("c"), true, Bytes.toBytes("d$%%"), false));
        filter = new MultiRowRangeFilter(ranges);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 2);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setReversed(true);
        ranges = new ArrayList<>();
        ranges.add(new MultiRowRangeFilter.RowRange(Bytes.toBytes("kb2"), false, Bytes.toBytes("kd4"), false));
        ranges.add(new MultiRowRangeFilter.RowRange(Bytes.toBytes("kb11"), true, Bytes.toBytes("kb2"), true));
        filter = new MultiRowRangeFilter(ranges);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 2);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setReversed(true);
        ranges = new ArrayList<>();
        ranges.add(new MultiRowRangeFilter.RowRange(Bytes.toBytes("z4"), true, Bytes.toBytes("zzzsad"), true));
        ranges.add(new MultiRowRangeFilter.RowRange(Bytes.toBytes("kb2"), true, Bytes.toBytes("kd4"), false));
        filter = new MultiRowRangeFilter(ranges);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 2);
        scanner.close();

        // Inclusive stop filter
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        InclusiveStopFilter iFilter = new InclusiveStopFilter(Bytes.toBytes("ka1"));
        scan.setFilter(iFilter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 7);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setReversed(true);
        scan.setMaxVersions(10);
        iFilter = new InclusiveStopFilter(Bytes.toBytes("ka1"));
        scan.setFilter(iFilter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 7);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        iFilter = new InclusiveStopFilter(Bytes.toBytes("kb1"));
        scan.setFilter(iFilter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 7);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        iFilter = new InclusiveStopFilter(Bytes.toBytes("kb2"));
        scan.setFilter(iFilter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 9);
        scanner.close();
    }

    @Test
    public void testColumnRangeFilter() throws Exception {
        String key1 = "ka1";
        String key2 = "kb2";
        String column1 = "abc";
        String column2 = "def";
        String value1 = "value1";
        String value2 = "value2";
        String family = "family1";
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.addFamily(toBytes(family));

        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);

        Scan scan;
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes("a"), true,
            Bytes.toBytes("b"), false);
        scan.setFilter(filter);
        ResultScanner scanner = hTable.getScanner(scan);

        int res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out
                    .printf(
                        "Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)), keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue)));
                res_count += 1;
            }
        }
        Assert.assertEquals(3, res_count);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        filter = new ColumnRangeFilter(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), false);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out
                    .printf(
                        "Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)), keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue)));
                res_count += 1;
            }
        }
        Assert.assertEquals(3, res_count);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        filter = new ColumnRangeFilter(Bytes.toBytes("abc"), false, Bytes.toBytes("def"), true);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out
                    .printf(
                        "Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)), keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue)));
                res_count += 1;
            }
        }
        Assert.assertEquals(6, res_count);
        scanner.close();

        // MultipleColumnPrefixFilter
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        byte[][] range = { Bytes.toBytes("g"), Bytes.toBytes("3"), Bytes.toBytes("d"), };
        MultipleColumnPrefixFilter iFilter = new MultipleColumnPrefixFilter(range);
        scan.setFilter(iFilter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out
                    .printf(
                        "Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)), keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue)));
                res_count += 1;
            }
        }
        Assert.assertEquals(6, res_count);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        // 和原生hbase不一致，已知
        range = new byte[][] { Bytes.toBytes("de"), Bytes.toBytes("bg"), Bytes.toBytes("nc"),
                Bytes.toBytes("aa"), Bytes.toBytes("abcd"), Bytes.toBytes("dea"), };
        iFilter = new MultipleColumnPrefixFilter(range);
        scan.setFilter(iFilter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out
                    .printf(
                        "Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)), keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue)));
                res_count += 1;
            }
        }
        Assert.assertEquals(6, res_count);
        scanner.close();
    }

    @Test
    public void testFilterNullRange() throws Exception {
        String key1 = "ka1";
        String key2 = "kb2";
        String column1 = "abc";
        String column2 = "def";
        String value1 = "value1";
        String value2 = "value2";
        String family = "family1";
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.addFamily(toBytes(family));

        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

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

        Scan scan;
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        byte[][] r = {};
        MultipleColumnPrefixFilter iFilter = new MultipleColumnPrefixFilter(r);
        scan.setFilter(iFilter);
        ResultScanner scanner = hTable.getScanner(scan);

        int res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out
                    .printf(
                        "Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)), keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue)));
                Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 0);
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
        String family = "family1";
        long   ts;
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.addFamily(toBytes(family));

        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

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
        Assert.assertEquals(1, r.rawCells().length);

        KeyOnlyFilter kFilter = new KeyOnlyFilter();
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(kFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.rawCells().length);

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
        Assert.assertEquals(1, r.rawCells().length);

        tss = new ArrayList<>();
        tss.add(ts - 1);
        tss.add(ts + 1);
        tFilter = new TimestampsFilter(tss);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(tFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        Put putKey1Column3Value1 = new Put(toBytes(key1));
        putKey1Column3Value1.addColumn(toBytes(family), toBytes("ggg"), toBytes(value1));
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
            for (Cell keyValue : result.rawCells()) {
                if (res_count < 8) {
                    Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(keyValue));
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
            for (Cell keyValue : result.rawCells()) {
                if (res_count < 1) {
                    Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(keyValue));
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
            for (Cell keyValue : result.rawCells()) {
                if (res_count < 8) {
                    Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(keyValue));
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
                    Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(cell));
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(cell));
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
                    Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(cell));
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(cell));
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
                    Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(cell));
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(cell));
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
                    Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(cell));
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(cell));
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
                    Assert.assertArrayEquals(key1.getBytes(), CellUtil.cloneRow(cell));
                } else {
                    Assert.assertArrayEquals(key2.getBytes(), CellUtil.cloneRow(cell));
                }
                Assert.assertEquals(0, cell.getValueLength());
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 10);
        scanner.close();
    }

    @Test
    public void testFuzzyRowFilter() throws Exception {
        String key1 = "abab";
        String key2 = "abcc";
        String column1 = "c1";
        String column2 = "c2";
        String column3 = "c3";
        String column4 = "c4";
        String column5 = "c5";
        String value1 = "value1";
        String value2 = "value2";
        String family = "family1";
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.addFamily(toBytes(family));

        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey1Column3Value1 = new Put(toBytes(key1));
        putKey1Column3Value1.addColumn(toBytes(family), toBytes(column3), toBytes(value1));

        Put putKey1Column4Value1 = new Put(toBytes(key1));
        putKey1Column4Value1.addColumn(toBytes(family), toBytes(column4), toBytes(value1));

        Put putKey1Column5Value1 = new Put(toBytes(key1));
        putKey1Column5Value1.addColumn(toBytes(family), toBytes(column5), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column3Value1);
        tryPut(hTable, putKey1Column4Value1);
        tryPut(hTable, putKey1Column5Value1);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);

        Scan scan;
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        List<Pair<byte[], byte[]>> fuzzyKey = new ArrayList<>();
        fuzzyKey.add(new Pair<>(Bytes.toBytes("abab"), Bytes.toBytes("0000")));
        fuzzyKey.add(new Pair<>(Bytes.toBytes("dddd"), Bytes.toBytes("0000")));
        FuzzyRowFilter filter = new FuzzyRowFilter(fuzzyKey);
        scan.setFilter(filter);
        ResultScanner scanner = hTable.getScanner(scan);

        int res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 10);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setReversed(true);
        fuzzyKey = new ArrayList<>();
        fuzzyKey.add(new Pair<>(Bytes.toBytes("dddd"), Bytes.toBytes("0000")));
        fuzzyKey.add(new Pair<>(Bytes.toBytes("abcc"), Bytes.toBytes("0000")));
        filter = new FuzzyRowFilter(fuzzyKey);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 2);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setReversed(true);
        fuzzyKey = new ArrayList<>();
        fuzzyKey.add(new Pair<>(Bytes.toBytes("ccab"), Bytes.toBytes("1100")));
        fuzzyKey.add(new Pair<>(Bytes.toBytes("dddd"), Bytes.toBytes("0000")));
        filter = new FuzzyRowFilter(fuzzyKey);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 10);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setReversed(true);
        fuzzyKey = new ArrayList<>();
        fuzzyKey.add(new Pair<>(Bytes.toBytes("cccc"), Bytes.toBytes("1100")));
        fuzzyKey.add(new Pair<>(Bytes.toBytes("dddd"), Bytes.toBytes("0000")));
        filter = new FuzzyRowFilter(fuzzyKey);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 2);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setReversed(true);
        fuzzyKey = new ArrayList<>();
        fuzzyKey.add(new Pair<>(Bytes.toBytes("ab##"), Bytes.toBytes("0011")));
        fuzzyKey.add(new Pair<>(Bytes.toBytes("dddd"), Bytes.toBytes("0000")));
        filter = new FuzzyRowFilter(fuzzyKey);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 12);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setReversed(true);
        fuzzyKey = new ArrayList<>();
        fuzzyKey.add(new Pair<>(Bytes.toBytes("azc"), Bytes.toBytes("010")));
        fuzzyKey.add(new Pair<>(Bytes.toBytes("dddd"), Bytes.toBytes("0000")));
        filter = new FuzzyRowFilter(fuzzyKey);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 2);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setReversed(true);
        fuzzyKey = new ArrayList<>();
        fuzzyKey.add(new Pair<>(Bytes.toBytes("azccd"), Bytes.toBytes("01001")));
        fuzzyKey.add(new Pair<>(Bytes.toBytes("dddd"), Bytes.toBytes("0000")));
        filter = new FuzzyRowFilter(fuzzyKey);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 2);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setReversed(true);
        fuzzyKey = new ArrayList<>();
        fuzzyKey.add(new Pair<>(Bytes.toBytes(""), Bytes.toBytes("")));
        fuzzyKey.add(new Pair<>(Bytes.toBytes("dddd"), Bytes.toBytes("0000")));
        filter = new FuzzyRowFilter(fuzzyKey);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 12);
        scanner.close();
    }

    @Test
    public void testFirstKeyValueMatchingQualifiersFilter() throws Exception {
        String key1 = "getKey1";
        String key2 = "getKey2";
        String column1 = "c1";
        String column2 = "c2";
        String column3 = "c3";
        String column4 = "c4";
        String column5 = "c5";
        String value1 = "value1";
        String value2 = "value2";
        String family = "family1";
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.addFamily(toBytes(family));

        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey1Column3Value1 = new Put(toBytes(key1));
        putKey1Column3Value1.addColumn(toBytes(family), toBytes(column3), toBytes(value1));

        Put putKey1Column4Value1 = new Put(toBytes(key1));
        putKey1Column4Value1.addColumn(toBytes(family), toBytes(column4), toBytes(value1));

        Put putKey1Column5Value1 = new Put(toBytes(key1));
        putKey1Column5Value1.addColumn(toBytes(family), toBytes(column5), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column3Value1);
        tryPut(hTable, putKey1Column4Value1);
        tryPut(hTable, putKey1Column5Value1);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);

        Scan scan;
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        TreeSet<byte []> qualifiers = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        qualifiers.add(Bytes.toBytes("c11"));
        qualifiers.add(Bytes.toBytes("c2"));
        FirstKeyValueMatchingQualifiersFilter filter = new FirstKeyValueMatchingQualifiersFilter(qualifiers);
        scan.setFilter(filter);
        ResultScanner scanner = hTable.getScanner(scan);

        int res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 5);
        scanner.close();
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setReversed(true);
        qualifiers = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        qualifiers.add(Bytes.toBytes("c22"));
        qualifiers.add(Bytes.toBytes("c4"));
        filter = new FirstKeyValueMatchingQualifiersFilter(qualifiers);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                res_count += 1;
            }
        }
        Assert.assertEquals(11, res_count);
        scanner.close();

        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scan.setReversed(true);
        qualifiers = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        qualifiers.add(Bytes.toBytes("c22"));
        qualifiers.add(Bytes.toBytes("a"));
        filter = new FirstKeyValueMatchingQualifiersFilter(qualifiers);
        scan.setFilter(filter);
        scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                System.out.printf("Rowkey: %s, Column Family: %s, Column Qualifier: %s, Timestamp: %d, Value: %s%n",
                        Bytes.toString(result.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(keyValue)),
                        Bytes.toString(CellUtil.cloneQualifier(keyValue)),
                        keyValue.getTimestamp(),
                        Bytes.toString(CellUtil.cloneValue(keyValue))
                );
                res_count += 1;
            }
        }
        Assert.assertEquals(res_count, 12);
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
        deleteKey1Family.addFamily(toBytes(family));

        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

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
        Assert.assertEquals(7, r.rawCells().length);

        // prefix filter excluded
        get = new Get(toBytes(key1));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        PrefixFilter prefixFilter = new PrefixFilter(toBytes("aa"));
        get.setFilter(prefixFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        // prefix filter included
        get = new Get(toBytes(key1));
        get.addFamily(toBytes(family));
        prefixFilter = new PrefixFilter(toBytes("get"));
        get.setFilter(prefixFilter);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        get = new Get(toBytes(key1));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        prefixFilter = new PrefixFilter(toBytes("get"));
        get.setFilter(prefixFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.rawCells().length);

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
        Assert.assertEquals(7, r.rawCells().length);

        // columnCountGetFilter filter 1
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        ColumnCountGetFilter columnCountGetFilter = new ColumnCountGetFilter(1);
        get.setFilter(columnCountGetFilter);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        DependentColumnFilter dependentColumnFilter = new DependentColumnFilter(
            Bytes.toBytes(family), Bytes.toBytes(column1));
        get.setFilter(dependentColumnFilter);
        r = hTable.get(get);
        Assert.assertEquals(3, r.rawCells().length);

        // columnCountGetFilter filter 2
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        columnCountGetFilter = new ColumnCountGetFilter(2);
        get.setFilter(columnCountGetFilter);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

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
        Assert.assertEquals(0, r.rawCells().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            toBytes(value1)));
        get.setFilter(valueFilter);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        hTable.put(putKey1Column1Value2);
        hTable.put(putKey1Column2Value2);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            toBytes(value2)));
        get.setFilter(valueFilter);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        valueFilter = new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(
            toBytes(value2)));
        get.setFilter(valueFilter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(
            toBytes(value1)));
        get.setFilter(valueFilter);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
            new BinaryComparator(toBytes(value1)));
        get.setFilter(valueFilter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(
            toBytes(value3)));
        get.setFilter(valueFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

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
        Assert.assertEquals(3, r.rawCells().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            toBytes(column2)));
        get.setFilter(qualifierFilter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.GREATER,
            new BinaryComparator(toBytes(column1)));
        get.setFilter(qualifierFilter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
            new BinaryComparator(toBytes(column1)));
        get.setFilter(qualifierFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.rawCells().length);

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
        Assert.assertEquals(1, r.rawCells().length);

        filterList = new FilterList();
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(family), Bytes
            .toBytes(column1), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value1)));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(7, r.rawCells().length);

        filterList = new FilterList();
        filterList.addFilter(new SingleColumnValueExcludeFilter(Bytes.toBytes(family), Bytes
            .toBytes(column1), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value1)));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        filterList = new FilterList();
        filterList.addFilter(new DependentColumnFilter(Bytes.toBytes(family), Bytes
            .toBytes(column1), false));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(3, r.rawCells().length);

        filterList = new FilterList();
        filterList.addFilter(new DependentColumnFilter(Bytes.toBytes(family), Bytes
            .toBytes(column2), false));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        filterList = new FilterList();
        filterList.addFilter(new DependentColumnFilter(Bytes.toBytes(family), Bytes
            .toBytes(column2)));
        get = new Get(toBytes(key2));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        filterList = new FilterList();
        filterList.addFilter(new DependentColumnFilter(Bytes.toBytes(family), Bytes
            .toBytes(column2), true));
        get = new Get(toBytes(key2));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        filterList = new FilterList();
        filterList.addFilter(new DependentColumnFilter(Bytes.toBytes(family), Bytes
            .toBytes(column2), false, CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            toBytes(value2))));
        get = new Get(toBytes(key2));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        filterList = new FilterList();
        filterList.addFilter(new ColumnCountGetFilter(1));
        filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.GREATER,
            new BinaryComparator(toBytes(column2))));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        filterList = new FilterList(MUST_PASS_ONE);
        filterList.addFilter(new ColumnCountGetFilter(2));
        filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(toBytes(column2))));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(6, r.rawCells().length);

        filterList = new FilterList();
        filterList.addFilter(new ColumnCountGetFilter(2));
        filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(toBytes(column2))));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        filterList = new FilterList();
        filterList.addFilter(new ColumnCountGetFilter(2));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);
        Assert.assertFalse(Bytes.equals(CellUtil.cloneQualifier(r.rawCells()[0]),
            CellUtil.cloneQualifier(r.rawCells()[1])));

        filterList = new FilterList();
        filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(toBytes(column2))));
        filterList.addFilter(new ColumnCountGetFilter(2));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        filterList = new FilterList();
        filterList.addFilter(new ColumnCountGetFilter(2));
        filterList.addFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            toBytes(value2))));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

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
        Assert.assertEquals(7, r.rawCells().length);

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
        Assert.assertEquals(1, r.rawCells().length);

        // test empty filter in FilterList
        filterList = new FilterList();
        emptyFilterList = new FilterList();
        filterList.addFilter(emptyFilterList);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(filterList);
        r = hTable.get(get);
        Assert.assertEquals(7, r.rawCells().length);

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
        Assert.assertEquals(7, r.rawCells().length);

        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter;
        singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes(family),
            Bytes.toBytes(column1), CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value1)));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueExcludeFilter);
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column1), CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value2)));
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column1), CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                toBytes(value2)));
        singleColumnValueFilter.setLatestVersionOnly(false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.rawCells().length);

        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column2), CompareFilter.CompareOp.LESS, new BinaryComparator(
                toBytes(value1)));
        singleColumnValueFilter.setLatestVersionOnly(false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column2), CompareFilter.CompareOp.LESS, new BinaryComparator(
                toBytes(value2)));
        singleColumnValueFilter.setLatestVersionOnly(false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.rawCells().length);

        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column2), CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(
                toBytes(value2)));
        singleColumnValueFilter.setLatestVersionOnly(false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.rawCells().length);

        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column2), CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(
                toBytes(value2)));
        singleColumnValueFilter.setLatestVersionOnly(false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(7, r.rawCells().length);

        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(column2), CompareFilter.CompareOp.GREATER, new BinaryComparator(
                toBytes(value2)));
        singleColumnValueFilter.setLatestVersionOnly(false);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(singleColumnValueFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

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
        Assert.assertEquals(2, r.rawCells().length);

        valueFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(
            toBytes(value2)));
        skipFilter = new SkipFilter(valueFilter);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(skipFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

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
        Assert.assertEquals(0, r.rawCells().length);

        valueFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(
            toBytes(value2)));
        skipFilter = new SkipFilter(valueFilter);
        get = new Get(toBytes(key1));
        get.setMaxVersions(10);
        get.addFamily(toBytes(family));
        get.setFilter(skipFilter);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

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
        Assert.assertEquals(1, r.rawCells().length);
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
        deleteKey1Family.addFamily(toBytes(family));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.addFamily(toBytes(family));
        Delete deleteKey4Family = new Delete(toBytes(key4));
        deleteKey4Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey4Column1Value1 = new Put(toBytes(key4));
        putKey4Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

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
    public void testScanSession() throws Exception {
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
        deleteKey1Family.addFamily(toBytes(family));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.addFamily(toBytes(family));
        Delete deleteKey4Family = new Delete(toBytes(key4));
        deleteKey4Family.addFamily(toBytes(family));
        Delete deleteKey5Family = new Delete(toBytes(key5));
        deleteKey5Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey4Column2Value1 = new Put(toBytes(key4));
        putKey4Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey5Column2Value1 = new Put(toBytes(key5));
        putKey5Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

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

        // The server defaults to a lease of 60 seconds. Therefore, at 20 seconds,
        // the transaction is checked to ensure it has not rolled back, and the lease is updated.
        // At 55 seconds, the query should still be able to retrieve the data and update the lease.
        // If it exceeds 60 seconds (at 61 seconds), the session is deleted.
        Thread.sleep(5 * 1000);
        scanner.next();
        Thread.sleep(20 * 1000);
        scanner.renewLease();

        Thread.sleep(41 * 1000);
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
        String family = "family1";

        // delete previous data
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.addFamily(toBytes(family));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.addFamily(toBytes(family));
        Delete deleteZKey1Family = new Delete(toBytes(zKey1));
        deleteZKey1Family.addFamily(toBytes(family));
        Delete deleteZKey2Family = new Delete(toBytes(zKey2));
        deleteZKey2Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey3Column1Value2 = new Put(toBytes(key3));
        putKey3Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column2Value2 = new Put(toBytes(key3));
        putKey3Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putzKey1Column1Value1 = new Put(toBytes(zKey1));
        putzKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putzKey2Column1Value1 = new Put(toBytes(zKey2));
        putzKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

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
        // +-----------+---------+----------------+--------+
        // | K         | Q       | T              | V      |
        // +-----------+---------+----------------+--------+
        // | scanKey1x | column1 | -1729223351579 | value1 |
        // | scanKey1x | column1 | -1729223351504 | value2 |
        // | scanKey1x | column1 | -1729223351431 | value1 |
        // | scanKey1x | column2 | -1729223351867 | value2 |
        // | scanKey1x | column2 | -1729223351796 | value1 |
        // | scanKey1x | column2 | -1729223351724 | value2 |
        // | scanKey1x | column2 | -1729223351651 | value1 |
        // | scanKey2x | column2 | -1729223352015 | value2 |
        // | scanKey2x | column2 | -1729223351941 | value1 |
        // | scanKey3x | column1 | -1729223352159 | value2 |
        // | scanKey3x | column1 | -1729223352088 | value1 |
        // | scanKey3x | column2 | -1729223352304 | value2 |
        // | scanKey3x | column2 | -1729223352232 | value1 |
        // | zScanKey1 | column1 | -1729223352378 | value1 |
        // | zScanKey2 | column1 | -1729223352450 | value1 |
        // +-----------+---------+----------------+--------+

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

        // test single cf setColumnFamilyTimeRange
        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
        hTable.delete(deleteZKey1Family);
        hTable.delete(deleteZKey2Family);

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

        putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), minTimeStamp, toBytes(value1));

        putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), timeStamp1, toBytes(value2));

        putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), timeStamp2, toBytes(value1));

        putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), timeStamp3, toBytes(value2));

        putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), timeStamp4, toBytes(value1));

        putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), timeStamp5, toBytes(value2));

        putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), timeStamp6, toBytes(value1));

        putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), timeStamp7, toBytes(value2));

        putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.addColumn(toBytes(family), toBytes(column1), timeStamp8, toBytes(value1));

        putKey3Column1Value2 = new Put(toBytes(key3));
        putKey3Column1Value2.addColumn(toBytes(family), toBytes(column1), timeStamp9, toBytes(value2));

        putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.addColumn(toBytes(family), toBytes(column2), timeStamp10, toBytes(value1));

        putKey3Column2Value2 = new Put(toBytes(key3));
        putKey3Column2Value2.addColumn(toBytes(family), toBytes(column2), timeStamp11, toBytes(value2));

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column1Value1);
        tryPut(hTable, putKey2Column1Value2);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);
        tryPut(hTable, putKey3Column1Value1);
        tryPut(hTable, putKey3Column1Value2);
        tryPut(hTable, putKey3Column2Value1);
        tryPut(hTable, putKey3Column2Value2);


        get = new Get(toBytes(key1));
        get.setColumnFamilyTimeRange(toBytes(family), minTimeStamp, maxTimeStamp);
        get.addFamily(toBytes(family));
        get.setMaxVersions();
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        get = new Get(toBytes(key1));
        get.setColumnFamilyTimeRange(toBytes(family), minTimeStamp, timeStamp2 + 1);
        get.addFamily(toBytes(family));
        get.setMaxVersions();
        r = hTable.get(get);
        Assert.assertEquals(3, r.rawCells().length);

        get = new Get(toBytes(key2));
        // set invalid timeRange
        get.setTimeRange(minTimeStamp, maxTimeStamp);
        get.setColumnFamilyTimeRange(toBytes(family), minTimeStamp, timeStamp5 + 1);
        get.addFamily(toBytes(family));
        get.setMaxVersions();
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        get = new Get(toBytes(key2));
        get.setColumnFamilyTimeRange(toBytes(family), timeStamp5, maxTimeStamp);
        get.addFamily(toBytes(family));
        get.setMaxVersions();
        r = hTable.get(get);
        Assert.assertEquals(3, r.rawCells().length);

        get = new Get(toBytes(key3));
        get.setColumnFamilyTimeRange(toBytes(family), timeStamp8, timeStamp8);
        get.addFamily(toBytes(family));
        get.setMaxVersions();
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        get = new Get(toBytes(key3));
        get.setColumnFamilyTimeRange(toBytes(family), timeStamp8, timeStamp9);
        get.addFamily(toBytes(family));
        get.setMaxVersions();
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        get = new Get(toBytes(key3));
        get.setColumnFamilyTimeRange(toBytes(family), timeStamp8, timeStamp9);
        get.setColumnFamilyTimeRange(toBytes("mockFamily"), timeStamp8, timeStamp9);
        get.addFamily(toBytes(family));
        get.setMaxVersions();
        final Get multiFamGet = get;
        Assert.assertThrows(IOException.class, () -> {
           hTable.get(multiFamGet);
        });

        get = new Get(toBytes(key3));
        get.setColumnFamilyTimeRange(toBytes("mockFamily"), timeStamp8, timeStamp9);
        get.addFamily(toBytes(family));
        get.setMaxVersions();
        final Get missFamGet = get;
        Assert.assertThrows(IOException.class, () -> {
            hTable.get(missFamGet);
        });

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
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
        String family = "family1";

        // delete previous data
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.addFamily(toBytes(family));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.addFamily(toBytes(family));
        Delete deleteZKey1Family = new Delete(toBytes(zKey1));
        deleteZKey1Family.addFamily(toBytes(family));
        Delete deleteZKey2Family = new Delete(toBytes(zKey2));
        deleteZKey2Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey3Column1Value2 = new Put(toBytes(key3));
        putKey3Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column2Value2 = new Put(toBytes(key3));
        putKey3Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putzKey1Column1Value1 = new Put(toBytes(zKey1));
        putzKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putzKey2Column1Value1 = new Put(toBytes(zKey2));
        putzKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

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
        // +-----------+---------+----------------+--------+
        // | K         | Q       | T              | V      |
        // +-----------+---------+----------------+--------+
        // | scanKey1x | column1 | -1729236392149 | value1 |
        // | scanKey1x | column1 | -1729236392078 | value2 |
        // | scanKey1x | column1 | -1729236392008 | value1 |
        // | scanKey1x | column2 | -1729236392436 | value2 |
        // | scanKey1x | column2 | -1729236392364 | value1 |
        // | scanKey1x | column2 | -1729236392291 | value2 |
        // | scanKey1x | column2 | -1729236392220 | value1 |
        // | scanKey2x | column2 | -1729236392576 | value2 |
        // | scanKey2x | column2 | -1729236392506 | value1 |
        // | scanKey3x | column1 | -1729236392720 | value2 |
        // | scanKey3x | column1 | -1729236392647 | value1 |
        // | scanKey3x | column2 | -1729236392861 | value2 |
        // | scanKey3x | column2 | -1729236392790 | value1 |
        // | zScanKey1 | column1 | -1729236392931 | value1 |
        // | zScanKey2 | column1 | -1729236393002 | value1 |
        // +-----------+---------+----------------+--------+

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
        Assert.assertEquals(7, r.rawCells().length);
        get = new Get(toBytes(key2));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);
        get = new Get(toBytes(key3));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey2x".getBytes());
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
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
            for (Cell keyValue : result.rawCells()) {
                Arrays.equals(key2.getBytes(), CellUtil.cloneRow(keyValue));
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
            for (Cell keyValue : result.rawCells()) {
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
            for (Cell keyValue : result.rawCells()) {
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
            delete.addFamily(toBytes(family));
            hTable.delete(delete);
        }

        // verify table is empty
        scan = new Scan("scanKey".getBytes(), HConstants.EMPTY_BYTE_ARRAY);
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
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

        // test single cf setColumnFamilyTimeRange
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

        putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), minTimeStamp, toBytes(value1));

        putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), timeStamp1, toBytes(value2));

        putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), timeStamp2, toBytes(value1));

        putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), timeStamp3, toBytes(value2));

        putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), timeStamp4, toBytes(value1));

        putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), timeStamp5, toBytes(value2));

        putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), timeStamp6, toBytes(value1));

        putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), timeStamp7, toBytes(value2));

        putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.addColumn(toBytes(family), toBytes(column1), timeStamp8, toBytes(value1));

        putKey3Column1Value2 = new Put(toBytes(key3));
        putKey3Column1Value2.addColumn(toBytes(family), toBytes(column1), timeStamp9, toBytes(value2));

        putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.addColumn(toBytes(family), toBytes(column2), timeStamp10, toBytes(value1));

        putKey3Column2Value2 = new Put(toBytes(key3));
        putKey3Column2Value2.addColumn(toBytes(family), toBytes(column2), timeStamp11, toBytes(value2));

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column1Value1);
        tryPut(hTable, putKey2Column1Value2);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);
        tryPut(hTable, putKey3Column1Value1);
        tryPut(hTable, putKey3Column1Value2);
        tryPut(hTable, putKey3Column2Value1);
        tryPut(hTable, putKey3Column2Value2);

        // scan key1 + key2
        scan = new Scan(toBytes(key1), toBytes(key3));
        scan.addFamily(toBytes(family));
        scan.setColumnFamilyTimeRange(toBytes(family), minTimeStamp, maxTimeStamp);
        scan.setMaxVersions();
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (Cell kv : result.rawCells()) {
                Assert.assertTrue(key1.equals(Bytes.toString(CellUtil.cloneRow(kv)))
                        || key2.equals(Bytes.toString(CellUtil.cloneRow(kv))));
                ++res_count;
            }
        }
        Assert.assertEquals(8, res_count);

        // scan key1
        scan = new Scan(toBytes(key1), toBytes(key2));
        scan.addFamily(toBytes(family));
        scan.setColumnFamilyTimeRange(toBytes(family), minTimeStamp, maxTimeStamp);
        scan.setMaxVersions();
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (Cell kv : result.rawCells()) {
                Assert.assertTrue(key1.equals(Bytes.toString(CellUtil.cloneRow(kv))));
                ++res_count;
            }
        }
        Assert.assertEquals(4, res_count);

        // scan key1
        scan = new Scan(toBytes(key1), toBytes(key2));
        scan.addFamily(toBytes(family));
        // set invalid timeRange
        scan.setTimeRange(minTimeStamp, maxTimeStamp);
        scan.setColumnFamilyTimeRange(toBytes(family), timeStamp1, timeStamp3);
        scan.setMaxVersions();
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (Cell kv : result.rawCells()) {
                Assert.assertTrue(key1.equals(Bytes.toString(CellUtil.cloneRow(kv))));
                ++res_count;
            }
        }
        Assert.assertEquals(2, res_count);

        // scan all
        scan = new Scan();
        scan.addFamily(toBytes(family));
        scan.setColumnFamilyTimeRange(toBytes(family), timeStamp2, timeStamp7);
        scan.setMaxVersions();
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (Cell kv : result.rawCells()) {
                Assert.assertTrue(key1.equals(Bytes.toString(CellUtil.cloneRow(kv)))
                        || key2.equals(Bytes.toString(CellUtil.cloneRow(kv))));
                ++res_count;
            }
        }
        Assert.assertEquals(5, res_count);

        scan = new Scan();
        scan.addFamily(toBytes(family));
        scan.setColumnFamilyTimeRange(toBytes(family), timeStamp3, timeStamp9);
        scan.setMaxVersions();
        scanner = hTable.getScanner(scan);
        res_count = 0;
        boolean foundKey3 = false;
        for (Result result : scanner) {
            for (Cell kv : result.rawCells()) {
                if (!foundKey3) {
                    if (key3.equals(Bytes.toString(CellUtil.cloneRow(kv)))) {
                        foundKey3 = true;
                    }
                }
                ++res_count;
            }
        }
        Assert.assertTrue(foundKey3);
        Assert.assertEquals(6, res_count);

        scan = new Scan();
        scan.addFamily(toBytes(family));
        scan.setColumnFamilyTimeRange(toBytes(family), timeStamp3, timeStamp9);
        scan.setColumnFamilyTimeRange(toBytes("mockFamily"), timeStamp3, timeStamp9);
        scan.setMaxVersions();
        final Scan multiFamScan = scan;
        Assert.assertThrows(IOException.class, () -> {
            hTable.getScanner(multiFamScan);
        });

        scan = new Scan();
        scan.addFamily(toBytes(family));
        scan.setColumnFamilyTimeRange(toBytes("mockFamily"), timeStamp3, timeStamp9);
        scan.setMaxVersions();
        final Scan missFamScan = scan;
        Assert.assertThrows(IOException.class, () -> {
            hTable.getScanner(missFamScan);
        });

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
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
        String family = "family1";

        // delete previous data
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.addFamily(toBytes(family));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.addFamily(toBytes(family));
        Delete deleteZKey1Family = new Delete(toBytes(zKey1));
        deleteZKey1Family.addFamily(toBytes(family));
        Delete deleteZKey2Family = new Delete(toBytes(zKey2));
        deleteZKey2Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey3Column1Value2 = new Put(toBytes(key3));
        putKey3Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column2Value2 = new Put(toBytes(key3));
        putKey3Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Scan scan;
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
            for (Cell keyValue : result.rawCells()) {
                Arrays.equals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(6, res_count);
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
            for (Cell keyValue : result.rawCells()) {
                Arrays.equals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(3, res_count);
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
            for (Cell keyValue : result.rawCells()) {
                Arrays.equals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(6, res_count);
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
            for (Cell keyValue : result.rawCells()) {
                Arrays.equals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(6, res_count);
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
            for (Cell keyValue : result.rawCells()) {
                Arrays.equals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(0, res_count);
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
            for (Cell keyValue : result.rawCells()) {
                Arrays.equals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(13, res_count);
        scanner.close();

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);

        // test single cf setColumnFamilyTimeRange
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

        putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), minTimeStamp, toBytes(value1));

        putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), timeStamp1, toBytes(value2));

        putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), timeStamp2, toBytes(value1));

        putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), timeStamp3, toBytes(value2));

        putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), timeStamp4, toBytes(value1));

        putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), timeStamp5, toBytes(value2));

        putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), timeStamp6, toBytes(value1));

        putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), timeStamp7, toBytes(value2));

        putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.addColumn(toBytes(family), toBytes(column1), timeStamp8, toBytes(value1));

        putKey3Column1Value2 = new Put(toBytes(key3));
        putKey3Column1Value2.addColumn(toBytes(family), toBytes(column1), timeStamp9, toBytes(value2));

        putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.addColumn(toBytes(family), toBytes(column2), timeStamp10, toBytes(value1));

        putKey3Column2Value2 = new Put(toBytes(key3));
        putKey3Column2Value2.addColumn(toBytes(family), toBytes(column2), timeStamp11, toBytes(value2));

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey2Column1Value1);
        tryPut(hTable, putKey2Column1Value2);
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);
        tryPut(hTable, putKey3Column1Value1);
        tryPut(hTable, putKey3Column1Value2);
        tryPut(hTable, putKey3Column2Value1);
        tryPut(hTable, putKey3Column2Value2);

        // scan key1
        scan = new Scan();
        scan.setStartRow(toBytes(key1));
        scan.setStopRow("scanKey0x".getBytes());
        scan.addFamily(family.getBytes());
        scan.setColumnFamilyTimeRange(toBytes(family), minTimeStamp, timeStamp3);
        scan.setReversed(true);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (Cell kv : result.rawCells()) {
                Assert.assertTrue(key1.equals(Bytes.toString(CellUtil.cloneRow(kv))));
                ++res_count;
            }
        }
        Assert.assertEquals(3, res_count);

        // scan key1 + key2
        scan = new Scan();
        scan.setStartRow(toBytes(key2));
        scan.setStopRow("scanKey0x".getBytes());
        scan.addFamily(toBytes(family));
        scan.setColumnFamilyTimeRange(toBytes(family), timeStamp2, timeStamp7);
        scan.setReversed(true);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (Cell kv : result.rawCells()) {
                Assert.assertTrue(key1.equals(Bytes.toString(CellUtil.cloneRow(kv)))
                        || key2.equals(Bytes.toString(CellUtil.cloneRow(kv))));
                ++res_count;
            }
        }
        Assert.assertEquals(5, res_count);

        // scan key2
        scan = new Scan();
        scan.setStartRow(toBytes(key2));
        scan.setStopRow(toBytes(key1));
        scan.addFamily(toBytes(family));
        // set invalid timeRange
        scan.setTimeRange(minTimeStamp, maxTimeStamp);
        scan.setColumnFamilyTimeRange(toBytes(family), timeStamp4, timeStamp6);
        scan.setReversed(true);
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (Cell kv : result.rawCells()) {
                Assert.assertTrue(key2.equals(Bytes.toString(CellUtil.cloneRow(kv))));
                ++res_count;
            }
        }
        Assert.assertEquals(2, res_count);

        // scan all
        scan = new Scan();
        scan.setStartRow(toBytes(key3));
        scan.setStopRow("scanKey0x".getBytes());
        scan.addFamily(toBytes(family));
        scan.setColumnFamilyTimeRange(toBytes(family), timeStamp2, timeStamp7);
        scan.setReversed(true);
        scan.setMaxVersions();
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (Cell kv : result.rawCells()) {
                Assert.assertTrue(key1.equals(Bytes.toString(CellUtil.cloneRow(kv)))
                        || key2.equals(Bytes.toString(CellUtil.cloneRow(kv))));
                ++res_count;
            }
        }
        Assert.assertEquals(5, res_count);

        scan = new Scan();
        scan.setStartRow(toBytes(key3));
        scan.setStopRow("scanKey0x".getBytes());
        scan.addFamily(toBytes(family));
        scan.setColumnFamilyTimeRange(toBytes(family), timeStamp3, timeStamp9);
        scan.setReversed(true);
        scan.setMaxVersions();
        scanner = hTable.getScanner(scan);
        res_count = 0;
        boolean foundKey3 = false;
        for (Result result : scanner) {
            for (Cell kv : result.rawCells()) {
                if (!foundKey3) {
                    if (key3.equals(Bytes.toString(CellUtil.cloneRow(kv)))) {
                        foundKey3 = true;
                    }
                }
                ++res_count;
            }
        }
        Assert.assertTrue(foundKey3);
        Assert.assertEquals(6, res_count);

        scan = new Scan();
        scan.setStartRow(toBytes(key3));
        scan.setStopRow("scanKey0x".getBytes());
        scan.addFamily(toBytes(family));
        scan.setColumnFamilyTimeRange(toBytes(family), timeStamp3, timeStamp9);
        scan.setColumnFamilyTimeRange(toBytes("mockFamily"), timeStamp3, timeStamp9);
        scan.setReversed(true);
        scan.setMaxVersions();
        final Scan errorScan = scan;
        Assert.assertThrows(IOException.class, () -> {
            hTable.getScanner(errorScan);
        });

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
        String family = "partitionFamily1";

        // delete previous data
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.addFamily(toBytes(family));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.addFamily(toBytes(family));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.addFamily(toBytes(family));
        Delete deleteZKey1Family = new Delete(toBytes(zKey1));
        deleteZKey1Family.addFamily(toBytes(family));
        Delete deleteZKey2Family = new Delete(toBytes(zKey2));
        deleteZKey2Family.addFamily(toBytes(family));

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey3Column1Value2 = new Put(toBytes(key3));
        putKey3Column1Value2.addColumn(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.addColumn(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column2Value2 = new Put(toBytes(key3));
        putKey3Column2Value2.addColumn(toBytes(family), toBytes(column2), toBytes(value2));

        Put putzKey1Column1Value1 = new Put(toBytes(zKey1));
        putzKey1Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

        Put putzKey2Column1Value1 = new Put(toBytes(zKey2));
        putzKey2Column1Value1.addColumn(toBytes(family), toBytes(column1), toBytes(value1));

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
        Assert.assertEquals(7, r.rawCells().length);

        get = new Get(toBytes(key2));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        get = new Get(toBytes(key3));
        get.addFamily(toBytes(family));
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);

        // verify simple scan across partition
        scan = new Scan();
        scan.addFamily(family.getBytes());
        scan.setStartRow("scanKey1x".getBytes());
        scan.setStopRow("scanKey2x".getBytes());
        scan.setMaxVersions(10);
        ResultScanner scanner = hTable.getScanner(scan);

        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
                Arrays.equals(key1.getBytes(), CellUtil.cloneRow(keyValue));
                res_count += 1;
            }
        }
        Assert.assertEquals(7, res_count);
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
            for (Cell keyValue : result.rawCells()) {
                Arrays.equals(key2.getBytes(), CellUtil.cloneRow(keyValue));
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
            for (Cell keyValue : result.rawCells()) {
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
            for (Cell keyValue : result.rawCells()) {
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
            delete.addFamily(toBytes(family));
            hTable.delete(delete);
        }

        // verify table is empty
        scan = new Scan("scanKey".getBytes(), HConstants.EMPTY_BYTE_ARRAY);
        scan.addFamily(family.getBytes());
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        res_count = 0;
        for (Result result : scanner) {
            for (Cell keyValue : result.rawCells()) {
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
            delete.addFamily("family2".getBytes());
            delete.addColumns("family1".getBytes(), "column1_1".getBytes(),
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
            put.addColumn("family1".getBytes(), "column1_1".getBytes(), "value2".getBytes());
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
            put.addColumn("family1".getBytes(), "column1_1".getBytes(), "value2".getBytes());
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
            put.addColumn("family2".getBytes(), "column1_1".getBytes(), "value2".getBytes());
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
        Get get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);
        Put put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
        hTable.put(put);
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), "value1".getBytes());
        boolean ret = hTable.checkAndPut(key.getBytes(), "family1".getBytes(), column.getBytes(),
            value.getBytes(), put);
        Assert.assertTrue(ret);

        ret = hTable.checkAndPut(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.LESS, "value1".getBytes(), put);
        Assert.assertFalse(ret);
        ret = hTable.checkAndPut(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.LESS_OR_EQUAL, "value1".getBytes(), put);
        Assert.assertTrue(ret);
        ret = hTable.checkAndPut(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.GREATER, "".getBytes(), put);
        Assert.assertFalse(ret);
        ret = hTable.checkAndPut(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.GREATER_OR_EQUAL, "".getBytes(), put);
        Assert.assertFalse(ret);

        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(3, r.rawCells().length);
        Assert.assertEquals("value1", Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));
    }

    @Test
    public void testCheckAndDelete() throws IOException {
        // delete 只支持删一行
        String key = "checkAndDeleteKey";
        String column = "checkAndaddColumn";
        String column2 = "checkAndaddColumn2";
        String value = "value";
        String family = "family1";
        Put put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
        hTable.put(put);

        // check delete column
        Delete delete = new Delete(key.getBytes());
        delete.addColumn(family.getBytes(), column.getBytes());
        boolean ret = hTable.checkAndDelete(key.getBytes(), family.getBytes(), column.getBytes(),
            value.getBytes(), delete);
        Assert.assertTrue(ret);
        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), "value6".getBytes());
        hTable.put(put);
        ret = hTable.checkAndDelete(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.LESS, "value5".getBytes(), delete);
        Assert.assertTrue(ret);
        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), "value5".getBytes());
        hTable.put(put);
        ret = hTable.checkAndDelete(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.LESS_OR_EQUAL, "value5".getBytes(), delete);
        Assert.assertTrue(ret);
        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), "value1".getBytes());
        hTable.put(put);
        ret = hTable.checkAndDelete(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.GREATER, "value1".getBytes(), delete);
        Assert.assertFalse(ret);
        ret = hTable.checkAndDelete(key.getBytes(), "family1".getBytes(), column.getBytes(),
            CompareFilter.CompareOp.GREATER_OR_EQUAL, "value1".getBytes(), delete);
        Assert.assertTrue(ret);

        Get get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        // check delete columns
        long t = System.currentTimeMillis();
        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), t, value.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), t + 1, value.getBytes());
        hTable.put(put);
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);
        delete = new Delete(key.getBytes());
        delete.addColumns(family.getBytes(), column.getBytes());
        ret = hTable.checkAndDelete(key.getBytes(), family.getBytes(), column.getBytes(),
            value.getBytes(), delete);
        Assert.assertTrue(ret);
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        // check delete family
        t = System.currentTimeMillis();
        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), t, value.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), t + 1, value.getBytes());
        put.addColumn(family.getBytes(), column2.getBytes(), t, value.getBytes());
        put.addColumn(family.getBytes(), column2.getBytes(), t + 1, value.getBytes());
        hTable.put(put);
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addFamily(family.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);
        delete = new Delete(key.getBytes());
        delete.addFamily(family.getBytes());
        ret = hTable.checkAndDelete(key.getBytes(), family.getBytes(), column.getBytes(),
            value.getBytes(), delete);
        Assert.assertTrue(ret);
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addFamily(family.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        // test CheckAndMutateBuilder
        delete = new Delete(key.getBytes());
        delete.addFamily(family.getBytes());
        hTable.delete(delete);
        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
        hTable.put(put);

        // check delete column
        delete = new Delete(key.getBytes());
        delete.addColumn(family.getBytes(), column.getBytes());
        RowMutations rowMutations = new RowMutations(key.getBytes());
        rowMutations.add(delete);
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column.getBytes(),
            CompareFilter.CompareOp.EQUAL, value.getBytes(), rowMutations);
        Assert.assertTrue(ret);
        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), "value6".getBytes());
        hTable.put(put);
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column.getBytes(),
            CompareFilter.CompareOp.LESS, "value5".getBytes(), rowMutations);
        Assert.assertTrue(ret);
        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), "value5".getBytes());
        hTable.put(put);
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column.getBytes(),
            CompareFilter.CompareOp.LESS_OR_EQUAL, "value5".getBytes(), rowMutations);
        Assert.assertTrue(ret);
        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), "value1".getBytes());
        hTable.put(put);
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column.getBytes(),
            CompareFilter.CompareOp.GREATER, "value1".getBytes(), rowMutations);
        Assert.assertFalse(ret);
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column.getBytes(),
            CompareFilter.CompareOp.GREATER_OR_EQUAL, "value1".getBytes(), rowMutations);
        Assert.assertTrue(ret);

        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        // check delete columns
        t = System.currentTimeMillis();
        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), t, value.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), t + 1, value.getBytes());
        hTable.put(put);
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);
        delete = new Delete(key.getBytes());
        delete.addColumns(family.getBytes(), column.getBytes());
        ret = hTable.checkAndDelete(key.getBytes(), family.getBytes(), column.getBytes(),
            CompareFilter.CompareOp.EQUAL, value.getBytes(), delete);
        Assert.assertTrue(ret);
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addColumn(family.getBytes(), column.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        // check delete family
        t = System.currentTimeMillis();
        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), t, value.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), t + 1, value.getBytes());
        put.addColumn(family.getBytes(), column2.getBytes(), t, value.getBytes());
        put.addColumn(family.getBytes(), column2.getBytes(), t + 1, value.getBytes());
        hTable.put(put);
        get = new Get(key.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        get.addFamily(family.getBytes());
        r = hTable.get(get);
        Assert.assertEquals(4, r.rawCells().length);
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

        long t = System.currentTimeMillis();
        // put
        Put put1 = new Put(key.getBytes());
        put1.addColumn(family.getBytes(), column1.getBytes(), t, value1.getBytes());
        put1.addColumn(family.getBytes(), column2.getBytes(), t, value2.getBytes());

        Put put2 = new Put(key.getBytes());
        put2.addColumn(family.getBytes(), column1.getBytes(), t + 3, value2.getBytes());
        put2.addColumn(family.getBytes(), column2.getBytes(), t + 3, value1.getBytes());

        Put put3 = new Put(key.getBytes());
        put3.addColumn(family.getBytes(), column1.getBytes(), t + 5, value1.getBytes());
        put3.addColumn(family.getBytes(), column2.getBytes(), t + 5, value2.getBytes());

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
        Assert.assertEquals(6, r.rawCells().length);

        t = System.currentTimeMillis() + 7;
        // put
        put1 = new Put(key.getBytes());
        put1.addColumn(family.getBytes(), column1.getBytes(), t, value1.getBytes());
        put1.addColumn(family.getBytes(), column2.getBytes(), t, value2.getBytes());

        put2 = new Put(key.getBytes());
        put2.addColumn(family.getBytes(), column1.getBytes(), t + 3, value2.getBytes());
        put2.addColumn(family.getBytes(), column2.getBytes(), t + 3, value1.getBytes());

        put3 = new Put(key.getBytes());
        put3.addColumn(family.getBytes(), column1.getBytes(), t + 5, value1.getBytes());
        put3.addColumn(family.getBytes(), column2.getBytes(), t + 5, value2.getBytes());
        rowMutations = new RowMutations(key.getBytes());
        rowMutations.add(put1);
        rowMutations.add(put2);
        rowMutations.add(put3);
        // test GREATER op
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column1.getBytes(),
            CompareFilter.CompareOp.GREATER, value1.getBytes(), rowMutations);
        Assert.assertFalse(ret);
        get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        r = hTable.get(get);
        Assert.assertEquals(6, r.rawCells().length);

        // test less op
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column1.getBytes(),
            CompareFilter.CompareOp.GREATER, value2.getBytes(), rowMutations);
        Assert.assertTrue(ret);
        get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        r = hTable.get(get);
        Assert.assertEquals(12, r.rawCells().length);

        t = System.currentTimeMillis() + 14;
        // put
        put1 = new Put(key.getBytes());
        put1.addColumn(family.getBytes(), column1.getBytes(), t, value1.getBytes());
        put1.addColumn(family.getBytes(), column2.getBytes(), t, value2.getBytes());

        put2 = new Put(key.getBytes());
        put2.addColumn(family.getBytes(), column1.getBytes(), t + 3, value2.getBytes());
        put2.addColumn(family.getBytes(), column2.getBytes(), t + 3, value1.getBytes());

        put3 = new Put(key.getBytes());
        put3.addColumn(family.getBytes(), column1.getBytes(), t + 5, value1.getBytes());
        put3.addColumn(family.getBytes(), column2.getBytes(), t + 5, value2.getBytes());
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
        Assert.assertEquals(18, r.rawCells().length);

        t = System.currentTimeMillis() + 21;
        // put
        put1 = new Put(key.getBytes());
        put1.addColumn(family.getBytes(), column1.getBytes(), t, value1.getBytes());
        put1.addColumn(family.getBytes(), column2.getBytes(), t, value2.getBytes());

        // delete
        Delete delete1 = new Delete(key.getBytes());
        delete1.addColumns(family.getBytes(), column1.getBytes());

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
        Assert.assertEquals(1, r.rawCells().length);

        get = new Get(key.getBytes());
        get.addColumn(family.getBytes(), column2.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        r = hTable.get(get);
        Assert.assertEquals(10, r.rawCells().length);

        // test CheckAndMutateBuilder
        Delete delete = new Delete(key.getBytes());
        delete.addFamily(family.getBytes());
        hTable.delete(delete);

        t = System.currentTimeMillis();
        // put
        put1 = new Put(key.getBytes());
        put1.addColumn(family.getBytes(), column1.getBytes(), t, value1.getBytes());
        put1.addColumn(family.getBytes(), column2.getBytes(), t, value2.getBytes());

        put2 = new Put(key.getBytes());
        put2.addColumn(family.getBytes(), column1.getBytes(), t + 3, value2.getBytes());
        put2.addColumn(family.getBytes(), column2.getBytes(), t + 3, value1.getBytes());

        put3 = new Put(key.getBytes());
        put3.addColumn(family.getBytes(), column1.getBytes(), t + 5, value1.getBytes());
        put3.addColumn(family.getBytes(), column2.getBytes(), t + 5, value2.getBytes());

        rowMutations = new RowMutations(key.getBytes());
        rowMutations.add(put1);
        rowMutations.add(put2);
        rowMutations.add(put3);

        //put data
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column1.getBytes(),
            CompareFilter.CompareOp.NOT_EQUAL, null, rowMutations);
        Assert.assertTrue(ret);

        get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        r = hTable.get(get);
        Assert.assertEquals(6, r.rawCells().length);

        t = System.currentTimeMillis() + 7;
        // put
        put1 = new Put(key.getBytes());
        put1.addColumn(family.getBytes(), column1.getBytes(), t, value1.getBytes());
        put1.addColumn(family.getBytes(), column2.getBytes(), t, value2.getBytes());

        put2 = new Put(key.getBytes());
        put2.addColumn(family.getBytes(), column1.getBytes(), t + 3, value2.getBytes());
        put2.addColumn(family.getBytes(), column2.getBytes(), t + 3, value1.getBytes());

        put3 = new Put(key.getBytes());
        put3.addColumn(family.getBytes(), column1.getBytes(), t + 5, value1.getBytes());
        put3.addColumn(family.getBytes(), column2.getBytes(), t + 5, value2.getBytes());
        rowMutations = new RowMutations(key.getBytes());
        rowMutations.add(put1);
        rowMutations.add(put2);
        rowMutations.add(put3);
        // test LESS op
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column1.getBytes(),
            CompareFilter.CompareOp.LESS, value1.getBytes(), rowMutations);
        Assert.assertFalse(ret);
        get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        r = hTable.get(get);
        Assert.assertEquals(6, r.rawCells().length);

        // test less op
        ret = hTable.checkAndMutate(key.getBytes(), family.getBytes(), column1.getBytes(),
            CompareFilter.CompareOp.GREATER, value2.getBytes(), rowMutations);
        Assert.assertTrue(ret);
        get = new Get(key.getBytes());
        get.addFamily(family.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        r = hTable.get(get);
        Assert.assertEquals(12, r.rawCells().length);

        t = System.currentTimeMillis() + 14;
        // put
        put1 = new Put(key.getBytes());
        put1.addColumn(family.getBytes(), column1.getBytes(), t, value1.getBytes());
        put1.addColumn(family.getBytes(), column2.getBytes(), t, value2.getBytes());

        put2 = new Put(key.getBytes());
        put2.addColumn(family.getBytes(), column1.getBytes(), t + 3, value2.getBytes());
        put2.addColumn(family.getBytes(), column2.getBytes(), t + 3, value1.getBytes());

        put3 = new Put(key.getBytes());
        put3.addColumn(family.getBytes(), column1.getBytes(), t + 5, value1.getBytes());
        put3.addColumn(family.getBytes(), column2.getBytes(), t + 5, value2.getBytes());
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
        Assert.assertEquals(18, r.rawCells().length);

        t = System.currentTimeMillis() + 21;
        // put
        put1 = new Put(key.getBytes());
        put1.addColumn(family.getBytes(), column1.getBytes(), t, value1.getBytes());
        put1.addColumn(family.getBytes(), column2.getBytes(), t, value2.getBytes());

        // delete
        delete1 = new Delete(key.getBytes());
        delete1.addColumns(family.getBytes(), column1.getBytes());

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
        Assert.assertEquals(1, r.rawCells().length);

        get = new Get(key.getBytes());
        get.addColumn(family.getBytes(), column2.getBytes());
        get.setMaxVersions(Integer.MAX_VALUE);
        r = hTable.get(get);
        Assert.assertEquals(10, r.rawCells().length);
    }

    @Test
    public void testAppend() throws IOException {
        String column = "appendColumn";
        String key = "appendKey";

        // append an absent column is not supported yet
        //        Append append = new Append(key.getBytes());
        //        append.add("family1".getBytes(), column.getBytes(), toBytes("_append"));
        //        Result r = hTable.append(append);
        //        Assert.assertEquals(1, r.rawCells().length);
        //        for (KeyValue kv : r.rawCells()) {
        //            Assert.assertEquals("_append", Bytes.toString(CellUtil.cloneValue(kv)));
        //        }

        Put put = new Put(key.getBytes());
        put.addColumn("family1".getBytes(), column.getBytes(), toBytes("value"));
        hTable.put(put);
        Append append = new Append(key.getBytes());
        append.addColumn("family1".getBytes(), column.getBytes(), toBytes("_append"));
        Result r = hTable.append(append);
        Assert.assertEquals(1, r.rawCells().length);
        for (Cell kv : r.rawCells()) {
            Assert.assertEquals("value_append", Bytes.toString(CellUtil.cloneValue(kv)));
        }
    }

    @Test
    public void testHbasePutDeleteCell() throws Exception {
        final byte[] rowKey = Bytes.toBytes("12345");
        final byte[] family = Bytes.toBytes("family1");

        Put put = new Put(rowKey);
        put.addColumn(family, Bytes.toBytes("A"), Bytes.toBytes("a"));
        put.addColumn(family, Bytes.toBytes("B"), Bytes.toBytes("b"));
        put.addColumn(family, Bytes.toBytes("C"), Bytes.toBytes("c"));
        put.addColumn(family, Bytes.toBytes("D"), Bytes.toBytes("d"));
        hTable.put(put);
        // get row back and assert the values
        Get get = new Get(rowKey);
        get.addFamily(family);
        Result result = hTable.get(get);
        assertTrue("Column A value should be a",
            Bytes.toString(result.getValue(family, Bytes.toBytes("A"))).equals("a"));
        assertTrue("Column B value should be b",
            Bytes.toString(result.getValue(family, Bytes.toBytes("B"))).equals("b"));
        assertTrue("Column C value should be c",
            Bytes.toString(result.getValue(family, Bytes.toBytes("C"))).equals("c"));
        assertTrue("Column D value should be d",
            Bytes.toString(result.getValue(family, Bytes.toBytes("D"))).equals("d"));
        // put the same row again with C column deleted
        put = new Put(rowKey);
        put.addColumn(family, Bytes.toBytes("A"), Bytes.toBytes("a1"));
        put.addColumn(family, Bytes.toBytes("B"), Bytes.toBytes("b1"));
        KeyValue marker = new KeyValue(rowKey, family, Bytes.toBytes("C"),
            HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn);
        put.addColumn(family, Bytes.toBytes("D"), Bytes.toBytes("d1"));
        put.add(marker);
        hTable.put(put);
        // get row back and assert the values
        get = new Get(rowKey);
        get.addFamily(family);
        result = hTable.get(get);
        assertTrue("Column A value should be a1",
            Bytes.toString(result.getValue(family, Bytes.toBytes("A"))).equals("a1"));
        assertTrue("Column B value should be b1",
            Bytes.toString(result.getValue(family, Bytes.toBytes("B"))).equals("b1"));
        System.out.println(result.getValue(family, Bytes.toBytes("C")));
        assertTrue("Column C should not exist", result.getValue(family, Bytes.toBytes("C")) == null);
        assertTrue("Column D value should be d1",
            Bytes.toString(result.getValue(family, Bytes.toBytes("D"))).equals("d1"));
    }

    @Test
    public void testCellTTL() throws Exception {
        String key1 = "key1";
        String column1 = "cf1";
        String column2 = "cf2";
        String column3 = "cf3";
        String family = "cellTTLFamily";
        String value1 = "v1";
        String value2 = "v2";
        String app = "app";

        Result r;
        Put put1 = new Put(key1.getBytes());
        put1.addColumn(family.getBytes(), column1.getBytes(), toBytes(11L));
        put1.setTTL(5000);
        Put put2 = new Put(key1.getBytes());
        put2.addColumn(family.getBytes(), column1.getBytes(), toBytes(22L));
        put2.addColumn(family.getBytes(), column2.getBytes(), toBytes(33L));
        put2.setTTL(10000);
        Put put3 = new Put(key1.getBytes());
        put3.addColumn(family.getBytes(), column1.getBytes(), toBytes(11L));
        put3.setTTL(-3000);
        Put put4 = new Put(key1.getBytes());
        put4.addColumn(family.getBytes(), column1.getBytes(), toBytes(11L));
        put4.setTTL(0);
        Put errorPut = new Put(key1.getBytes());
        errorPut.addColumn("family1".getBytes(), column1.getBytes(), toBytes(11L));
        errorPut.setTTL(10);

        Get get = new Get(key1.getBytes());
        get.addFamily(family.getBytes());
        get.setMaxVersions(10);
        try {
            tryPut(hTable, errorPut);
        } catch (Exception e) {
            assertTrue(e.getCause().toString().contains("Unknown column 'TTL'"));
        }
        // test put and get
        tryPut(hTable, put1);
        tryPut(hTable, put2);
        tryPut(hTable, put3);
        tryPut(hTable, put4);
        r = hTable.get(get);
        assertEquals(3, r.size());
        Thread.sleep(5000);
        r = hTable.get(get);
        assertEquals(2, r.size());
        Thread.sleep(5000);
        r = hTable.get(get);
        assertEquals(0, r.size());

        // test increment
        tryPut(hTable, put1);
        tryPut(hTable, put2);
        Thread.sleep(1000);
        Increment increment = new Increment(key1.getBytes());
        increment.addColumn(family.getBytes(), column1.getBytes(), 1L);
        increment.addColumn(family.getBytes(), column2.getBytes(), 2L);
        increment.addColumn(family.getBytes(), column3.getBytes(), 5L);
        increment.setTTL(-5000);
        hTable.increment(increment);
        increment.setTTL(5000);
        hTable.increment(increment);
        get.setMaxVersions(1);
        r = hTable.get(get);

        assertEquals(3, r.size());
        assertEquals(23L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
            column1.getBytes()).get(0))));
        assertEquals(35L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
            column2.getBytes()).get(0))));
        assertEquals(5L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
            column3.getBytes()).get(0))));

        Thread.sleep(10000);
        r = hTable.get(get);
        assertEquals(0, r.size());

        increment = new Increment(key1.getBytes());
        increment.addColumn(family.getBytes(), column1.getBytes(), 1L);
        increment.addColumn(family.getBytes(), column2.getBytes(), 2L);
        increment.setTTL(5000);
        hTable.increment(increment);
        r = hTable.get(get);

        assertEquals(2, r.size());
        assertEquals(1L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
            column1.getBytes()).get(0))));
        assertEquals(2L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
            column2.getBytes()).get(0))));

        Thread.sleep(5000);
        r = hTable.get(get);
        assertEquals(0, r.size());

        tryPut(hTable, put1);
        tryPut(hTable, put2);
        increment.addColumn(family.getBytes(), column1.getBytes(), 4L);
        hTable.increment(increment);

        r = hTable.get(get);
        assertEquals(2, r.size());
        assertEquals(26L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
            column1.getBytes()).get(0))));
        assertEquals(35L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
            column2.getBytes()).get(0))));

        // test append
        Thread.sleep(10000);
        r = hTable.get(get);
        assertEquals(0, r.size());

        put3 = new Put(key1.getBytes());
        put3.addColumn(family.getBytes(), column1.getBytes(), toBytes(value1));
        put3.addColumn(family.getBytes(), column2.getBytes(), toBytes(value2));
        put3.setTTL(10000);
        tryPut(hTable, put3);

        Append append = new Append(key1.getBytes());
        KeyValue kv = new KeyValue(key1.getBytes(), family.getBytes(), column1.getBytes(),
            app.getBytes());
        append.add(kv);
        append.setTTL(-3000);
        hTable.append(append);
        append.setTTL(3000);
        hTable.append(append);

        r = hTable.get(get);
        assertEquals(2, r.size());
        assertEquals(
            value1 + app,
            Bytes.toString(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column1.getBytes()).get(0))));

        Thread.sleep(3000);
        r = hTable.get(get);
        assertEquals(2, r.size());
        assertEquals(
            value1,
            Bytes.toString(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column1.getBytes()).get(0))));

        Thread.sleep(7000);
        r = hTable.get(get);
        assertEquals(0, r.size());

        append.add(family.getBytes(), column1.getBytes(), app.getBytes());
        hTable.append(append);
        r = hTable.get(get);
        assertEquals(1, r.size());
        assertEquals(
            app,
            Bytes.toString(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column1.getBytes()).get(0))));

        Thread.sleep(3000);
        append.add(family.getBytes(), column2.getBytes(), app.getBytes());
        hTable.append(append);
        r = hTable.get(get);
        assertEquals(2, r.size());
        assertEquals(
            app,
            Bytes.toString(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column1.getBytes()).get(0))));
        assertEquals(
            app,
            Bytes.toString(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column2.getBytes()).get(0))));

        // test checkAndMutate
        Thread.sleep(3000);
        r = hTable.get(get);
        assertEquals(0, r.size());

        tryPut(hTable, put1);
        RowMutations rowMutations = new RowMutations(key1.getBytes());
        rowMutations.add(put2);
        Delete delete = new Delete(key1.getBytes());
        delete.addColumn(family.getBytes(), column1.getBytes());
        rowMutations.add(delete);
        boolean succ = hTable.checkAndMutate(key1.getBytes(), family.getBytes(),
            column1.getBytes(), CompareFilter.CompareOp.EQUAL, toBytes(11L), rowMutations);
        assertTrue(succ);
        r = hTable.get(get);
        assertEquals(r.size(), 2);
        assertEquals(11L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
            column1.getBytes()).get(0))));
        assertEquals(33L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
            column2.getBytes()).get(0))));

        Thread.sleep(10000);
        r = hTable.get(get);
        assertEquals(r.size(), 0);

        tryPut(hTable, put1);
        rowMutations = new RowMutations(key1.getBytes());
        put4 = new Put(key1.getBytes());
        put4.addColumn(family.getBytes(), column1.getBytes(), toBytes(22L));
        put4.addColumn(family.getBytes(), column2.getBytes(), toBytes(33L));
        put4.setTTL(10000);
        rowMutations.add(put4);
        succ = hTable.checkAndMutate(key1.getBytes(), family.getBytes(), column1.getBytes(),
            CompareFilter.CompareOp.EQUAL, toBytes(1L), rowMutations);
        assertFalse(succ);
        succ = hTable.checkAndMutate(key1.getBytes(), family.getBytes(), column1.getBytes(),
            CompareFilter.CompareOp.EQUAL, toBytes(11L), rowMutations);
        assertTrue(succ);

        r = hTable.get(get);
        assertEquals(r.size(), 2);
        assertEquals(22L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
            column1.getBytes()).get(0))));
        assertEquals(33L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
            column2.getBytes()).get(0))));

        Thread.sleep(5000);
        r = hTable.get(get);
        assertEquals(2, r.size());
        assertEquals(22L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
            column1.getBytes()).get(0))));
        assertEquals(33L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
            column2.getBytes()).get(0))));

        Thread.sleep(5000);
        r = hTable.get(get);
        assertEquals(r.size(), 0);
        put1 = new Put(key1.getBytes());
        put1.addColumn(family.getBytes(), column1.getBytes(), toBytes(11L));
        tryPut(hTable, put1);

        increment = new Increment(key1.getBytes());
        increment.addColumn(family.getBytes(), column1.getBytes(), 1L);
        hTable.increment(increment);
        r = hTable.get(get);
        assertEquals(r.size(), 1);
    }

    @Test
    public void testIncrement() throws IOException {
        String column = "incrementColumn";
        String key = "incrementKey";

        // increment an absent column is not supported yet
        //        Increment increment = new Increment(key.getBytes());
        //        increment.addColumn("family1".getBytes(), column.getBytes(), 1);
        //        Result r = hTable.increment(increment);
        //        Assert.assertEquals(1, r.rawCells().length);
        //        for (KeyValue kv : r.rawCells()) {
        //            Assert.assertEquals(1L, Bytes.toLong(CellUtil.cloneValue(kv)));
        //        }

        long timestamp = System.currentTimeMillis();
        Put put = new Put(key.getBytes());
        put.addColumn("family1".getBytes(), column.getBytes(), timestamp, toBytes(1L));
        put.addColumn("family1".getBytes(), column.getBytes(), timestamp + 10, toBytes(1L));
        hTable.put(put);
        Increment increment = new Increment(key.getBytes());
        increment.addColumn("family1".getBytes(), column.getBytes(), 1);
        Result r = hTable.increment(increment);
        Assert.assertEquals(1, r.rawCells().length);
        for (Cell kv : r.rawCells()) {
            Assert.assertEquals(2L, Bytes.toLong(CellUtil.cloneValue(kv)));
        }

        // increment an absent column is not supported yet
        //        increment = new Increment(key.getBytes());
        //        increment.addColumn("family1".getBytes(), column.getBytes(), 1);
        //        increment.setTimeRange(timestamp + 3, timestamp + 8);
        //        r = hTable.increment(increment);
        //        Assert.assertEquals(1, r.rawCells().length);
        //        for (KeyValue kv : r.rawCells()) {
        //            Assert.assertEquals(1L, Bytes.toLong(CellUtil.cloneValue(kv)));
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

        Get get = new Get(key.getBytes());
        get.addFamily("family1".getBytes());
        Assert.assertFalse(hTable.exists(get));

        long timestamp = System.currentTimeMillis();
        Put put = new Put(key.getBytes());
        put.addColumn("family1".getBytes(), column.getBytes(), timestamp, "value".getBytes());
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
    }

    @Ignore
    public void testMutateRow() throws IOException {

        String column1 = "mutationRowColumn1";
        String column2 = "mutationRowColumn2";
        String key = "mutationRowKey";
        String family1 = "family1";
        String value = "value";

        Delete deleteFamily = new Delete(key.getBytes());
        deleteFamily.addFamily(toBytes(family1));

        Get family1Get = new Get(toBytes(key));
        family1Get.addFamily(toBytes(family1));

        Delete addColumn1 = new Delete(key.getBytes());
        addColumn1.addColumns(toBytes(family1), toBytes(column1));

        Delete addColumn2 = new Delete(key.getBytes());
        addColumn2.addColumns(toBytes(family1), toBytes(column2));

        Put putColumn1 = new Put(toBytes(key));
        putColumn1.addColumn(toBytes(family1), toBytes(column1), toBytes(value));

        Put putColumn2 = new Put(toBytes(key));
        putColumn2.addColumn(toBytes(family1), toBytes(column2), toBytes(value));

        hTable.delete(deleteFamily);
        RowMutations rm1 = new RowMutations(toBytes(key));
        rm1.add(putColumn1);
        hTable.mutateRow(rm1);
        Result r1 = hTable.get(family1Get);
        Assert.assertEquals(1, r1.rawCells().length);
        Cell r1KV = r1.rawCells()[0];
        Assert.assertEquals(key, Bytes.toString(CellUtil.cloneRow(r1KV)));
        Assert.assertEquals(family1, Bytes.toString(CellUtil.cloneFamily(r1KV)));
        Assert.assertEquals(column1, Bytes.toString(CellUtil.cloneQualifier(r1KV)));
        Assert.assertEquals(value, Bytes.toString(CellUtil.cloneValue(r1KV)));

        hTable.delete(deleteFamily);
        RowMutations rm2 = new RowMutations(toBytes(key));
        rm2.add(putColumn2);
        rm2.add(putColumn1);
        hTable.mutateRow(rm2);
        Result r2 = hTable.get(family1Get);
        Assert.assertEquals(2, r2.rawCells().length);
        hTable.delete(deleteFamily);

        hTable.put(putColumn1);
        RowMutations rm3 = new RowMutations(toBytes(key));
        rm3.add(addColumn1);
        hTable.mutateRow(rm3);
        Result r3 = hTable.get(family1Get);
        Assert.assertEquals(0, r3.rawCells().length);

        hTable.put(putColumn1);
        hTable.put(putColumn2);
        RowMutations rm4 = new RowMutations(toBytes(key));
        rm4.add(addColumn1);
        rm4.add(addColumn2);
        hTable.mutateRow(rm4);
        Result r4 = hTable.get(family1Get);
        Assert.assertEquals(0, r4.rawCells().length);

        hTable.put(putColumn1);
        RowMutations rm5 = new RowMutations(toBytes(key));
        rm5.add(putColumn2);
        rm5.add(addColumn1);
        hTable.mutateRow(rm5);
        Result r5 = hTable.get(family1Get);
        Assert.assertEquals(1, r5.rawCells().length);
        Cell r5KV = r5.rawCells()[0];
        Assert.assertEquals(key, Bytes.toString(CellUtil.cloneRow(r5KV)));
        Assert.assertEquals(family1, Bytes.toString(CellUtil.cloneFamily(r5KV)));
        Assert.assertEquals(column2, Bytes.toString(CellUtil.cloneQualifier(r5KV)));
        Assert.assertEquals(value, Bytes.toString(CellUtil.cloneValue(r5KV)));
        hTable.delete(deleteFamily);
    }

    @Test
    public void testQualifyNull() throws Exception {
        // delete 只支持删一行
        String key = "qualifyNullKey";
        String value = "value";
        String value1 = "value1";
        String family = "family1";
        Put put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), null, value.getBytes());
        hTable.put(put);

        Get get = new Get(key.getBytes());
        get.addColumn(Bytes.toBytes(family), null);
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals(key, Bytes.toString(CellUtil.cloneRow(r.rawCells()[0])));
        Assert.assertEquals(value, Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));
        Assert.assertEquals(0, r.rawCells()[0].getQualifierLength());

        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), null, value1.getBytes());

        hTable.checkAndPut(Bytes.toBytes(key), Bytes.toBytes(family), null, Bytes.toBytes(value),
            put);

        get = new Get(key.getBytes());
        get.addColumn(Bytes.toBytes(family), null);
        r = hTable.get(get);
        for (Cell kv : r.rawCells()) {
            System.out.println("K = [" + Bytes.toString(CellUtil.cloneRow(kv)) + "] Q =["
                               + Bytes.toString(CellUtil.cloneQualifier(kv)) + "] T = ["
                               + kv.getTimestamp() + "] V = ["
                               + Bytes.toString(CellUtil.cloneValue(kv)) + "]");
        }
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals(key, Bytes.toString(CellUtil.cloneRow(r.rawCells()[0])));
        Assert.assertEquals(value1, Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));
        Assert.assertEquals(0, CellUtil.cloneQualifier(r.rawCells()[0]).length);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), null);
        scan.setMaxVersions(1);
        scan.setStartRow("qualifyNullKey".getBytes());
        scan.setStopRow("qualifyNullKeyA".getBytes());
        List<Cell> resultList = new ArrayList<Cell>();
        ResultScanner scanner = hTable.getScanner(scan);

        while ((r = scanner.next()) != null) {
            resultList.addAll(Arrays.asList(r.rawCells()));
        }
        Assert.assertEquals(1, resultList.size());

        scan.setMaxVersions(10);
        resultList = new ArrayList<Cell>();
        scanner = hTable.getScanner(scan);

        while ((r = scanner.next()) != null) {
            resultList.addAll(Arrays.asList(r.rawCells()));
        }

        Assert.assertEquals(2, resultList.size());

        Delete addColumn = new Delete(key.getBytes());
        addColumn.addColumn(Bytes.toBytes(family), null);
        hTable.delete(addColumn);

        get = new Get(key.getBytes());
        get.addColumn(Bytes.toBytes(family), null);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        Delete addColumns = new Delete(key.getBytes());
        addColumns.addColumns(Bytes.toBytes(family), null);
        hTable.delete(addColumns);

        get = new Get(key.getBytes());
        get.addColumn(Bytes.toBytes(family), null);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);
    }

    @Test
    public void testFamilyBlank() throws Exception {
        // delete 只支持删一行
        String key = "qualifyNullKey";
        String value = "value";
        String family = "   ";
        Delete delete = new Delete(key.getBytes());
        delete.addFamily(family.getBytes());
        try {
            hTable.delete(delete);
            fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("family is blank"));
        } catch (NoSuchColumnFamilyException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }
        Put put = new Put(key.getBytes());
        expectedException.expect(NullPointerException.class);
        put.addColumn(null, null, value.getBytes());
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
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("zero columns specified"));
        }

        Increment increment = new Increment(key.getBytes());
        // increment.addColumn(null, null, 1);
        try {
            hTable.increment(increment);
            fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("zero columns specified"));
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
        Put put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), Bytes.toBytes(column), value.getBytes());
        hTable.put(put);

        Get get = new Get(key.getBytes());
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);
        Assert.assertEquals(key, Bytes.toString(CellUtil.cloneRow(r.rawCells()[0])));
        Assert.assertEquals(value, Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));

        put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), Bytes.toBytes(column), value1.getBytes());

        hTable.checkAndPut(Bytes.toBytes(key), Bytes.toBytes(family), Bytes.toBytes(column),
            Bytes.toBytes(value), put);

        get = new Get(key.getBytes());
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);
        Assert.assertEquals(key, Bytes.toString(CellUtil.cloneRow(r.rawCells()[0])));
        Assert.assertEquals(value1, Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        scan.setMaxVersions(1);
        scan.setStartRow(("scannerMultiVersion1").getBytes());
        scan.setStopRow(("scannerMultiVersion9").getBytes());
        List<Cell> resultList = new ArrayList<Cell>();
        ResultScanner scanner = hTable.getScanner(scan);

        while ((r = scanner.next()) != null) {
            resultList.addAll(Arrays.asList(r.rawCells()));
        }
        Assert.assertEquals(1, resultList.size());

        scan.setMaxVersions(10);
        resultList = new ArrayList<Cell>();
        scanner = hTable.getScanner(scan);

        while ((r = scanner.next()) != null) {
            resultList.addAll(Arrays.asList(r.rawCells()));
        }

        Assert.assertEquals(2, resultList.size());
    }

    @Test
    public void testPutColumnFamilyNull() throws Exception {
        Put put1 = new Put(("key_c_f").getBytes());
        expectedException.expect(NullPointerException.class);
        put1.addColumn(null, ("column1").getBytes(), "value1_family_null".getBytes());
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("family is empty");
        hTable.put(put1);
    }

    @Test
    public void testPutColumnFamilyEmpty() throws Exception {
        Put put2 = new Put(("key_c_f").getBytes());
        put2.addColumn("".getBytes(), ("column1").getBytes(), "value1_family_empty".getBytes());
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("family is empty");
        hTable.put(put2);
    }

    @Test
    public void testPutColumnFamilySpace() throws Exception {
        Put put3 = new Put(("key_c_f").getBytes());
        put3.addColumn("  ".getBytes(), ("column1").getBytes(), "value1_family_space".getBytes());
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("family is blank");
        hTable.put(put3);
    }

    @Test
    public void testPutColumnFamilyNotExists() throws Exception {
        /** family 不存在时提示不友好，*/
        Put put4 = new Put(("key_c_f").getBytes());
        put4.addColumn("family_not_exists".getBytes(), "column2".getBytes(),
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
        put.addColumn("family1".getBytes(), null, "value1_qualifier_null".getBytes());
        hTable.put(put);
        Get get = new Get("key_c_q_null".getBytes());
        get.addColumn("family1".getBytes(), null);
        //get.setMaxVersions();
        Result result = hTable.get(get);
        Assert.assertEquals("", 1, result.rawCells().length);
        Assert.assertEquals("", 0, result.rawCells()[0].getQualifierLength());
        Assert.assertArrayEquals("", "value1_qualifier_null".getBytes(),
            CellUtil.cloneValue(result.rawCells()[0]));

        put = new Put(("key_c_q_empty").getBytes());
        put.addColumn("family1".getBytes(), "".getBytes(), "value1_qualifier_empty".getBytes());
        hTable.put(put);
        get = new Get("key_c_q_empty".getBytes());
        get.addColumn("family1".getBytes(), "".getBytes());
        result = hTable.get(get);
        Assert.assertEquals("", 1, result.rawCells().length);
        Assert.assertEquals("", 0, result.rawCells()[0].getQualifierLength());
        Assert.assertArrayEquals("", "value1_qualifier_empty".getBytes(),
            CellUtil.cloneValue(result.rawCells()[0]));

        put = new Put(("key_c_q_space").getBytes());
        put.addColumn("family1".getBytes(), "  ".getBytes(), "value1_qualifier_space".getBytes());
        hTable.put(put);
        get = new Get("key_c_q_space".getBytes());
        get.addColumn("family1".getBytes(), "  ".getBytes());
        result = hTable.get(get);
        Assert.assertEquals("", 1, result.rawCells().length);
        Assert.assertArrayEquals("", "value1_qualifier_space".getBytes(),
            CellUtil.cloneValue(result.rawCells()[0]));
        //空格可以做qualifier
        Assert.assertEquals("", 2, result.rawCells()[0].getQualifierLength());

        get = new Get("key_c_q_space".getBytes());
        get.addColumn("family1".getBytes(), " ".getBytes());
        result = hTable.get(get);
        //一个空格的qualifier不能取出两个空格的qualifier的值
        Assert.assertEquals("", 0, result.rawCells().length);

        get = new Get("key_c_q_space".getBytes());
        get.addColumn("family1".getBytes(), "".getBytes());
        result = hTable.get(get);
        //empty字符串qualifier不能取出两个空格的qualifier的值
        Assert.assertEquals("", 0, result.rawCells().length);

    }

    //    @Test
    public void testTTLColumnLevel() throws Exception {
        Put put = new Put(("key_ttl_column").getBytes());
        put.addColumn("family_ttl".getBytes(), ("column1").getBytes(),
            "column1_value1_ttl_column".getBytes());
        put.addColumn("family_ttl".getBytes(), ("column2").getBytes(),
            "column2_value1_ttl_column".getBytes());
        hTable.put(put);
        Get get = new Get("key_ttl_column".getBytes());
        get.addColumn("family_ttl".getBytes(), "column1".getBytes());
        get.addColumn("family_ttl".getBytes(), "column2".getBytes());
        get.setMaxVersions(1);
        Result result = hTable.get(get);
        Assert.assertEquals("", 2, result.rawCells().length);
        Assert.assertEquals("", 1,
            result.getColumnCells("family_ttl".getBytes(), ("column1").getBytes()).size());
        Assert.assertEquals("", 1,
            result.getColumnCells("family_ttl".getBytes(), ("column2").getBytes()).size());
        Assert.assertArrayEquals(
            "",
            "column1_value1_ttl_column".getBytes(),
            CellUtil.cloneValue(result.getColumnCells("family_ttl".getBytes(),
                ("column1").getBytes()).get(0)));
        Assert.assertArrayEquals(
            "",
            "column2_value1_ttl_column".getBytes(),
            CellUtil.cloneValue(result.getColumnCells("family_ttl".getBytes(),
                ("column2").getBytes()).get(0)));

        //过期之后不能再查出数据
        Thread.sleep(4 * 1000L);
        result = hTable.get(get);
        Assert.assertEquals("", 0, result.rawCells().length);
    }

    //    @Test
    public void testTTLFamilyLevel() throws Exception {
        Put put = new Put(("key_ttl_family").getBytes());
        put.addColumn("family_ttl".getBytes(), ("column1").getBytes(),
            "column1_value_ttl_family".getBytes());
        put.addColumn("family_ttl".getBytes(), ("column2").getBytes(),
            "column2_value_ttl_family".getBytes());
        hTable.put(put);
        Get get = new Get("key_ttl_family".getBytes());
        get.addFamily("family_ttl".getBytes());
        get.setMaxVersions(1);
        Result result = hTable.get(get);
        Assert.assertEquals("", 2, result.rawCells().length);
        Assert.assertEquals("", 1,
            result.getColumnCells("family_ttl".getBytes(), ("column1").getBytes()).size());
        Assert.assertEquals("", 1,
            result.getColumnCells("family_ttl".getBytes(), ("column2").getBytes()).size());
        Assert.assertArrayEquals(
            "",
            "column1_value_ttl_family".getBytes(),
            CellUtil.cloneValue(result.getColumnCells("family_ttl".getBytes(),
                ("column1").getBytes()).get(0)));
        Assert.assertArrayEquals(
            "",
            "column2_value_ttl_family".getBytes(),
            CellUtil.cloneValue(result.getColumnCells("family_ttl".getBytes(),
                ("column2").getBytes()).get(0)));

        //过期之后不能再查出数据
        Thread.sleep(4 * 1000L);
        result = hTable.get(get);
        Assert.assertEquals("", 0, result.rawCells().length);
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

        for (int i = 0; i < 100; i++) {
            Increment increment = new Increment(key.getBytes());
            increment.addColumn("family1".getBytes(), column.getBytes(), 1);
            Thread t = new Thread(new IncrementHelper(hTable, increment));
            t.start();
        }
        Thread.sleep(300);
        Put put = new Put(key.getBytes());
        put.addColumn("family1".getBytes(), column.getBytes(), toByteArray(1));
        Thread t = new Thread(new PutHelper(hTable, put));
        t.start();
        t.join();

        Thread.sleep(8000);

        Get get = new Get("incrementKey".getBytes());
        get.addColumn("family1".getBytes(), "incrementColumn".getBytes());
        Result result = hTable.get(get);
        Cell kv = result.rawCells()[0];
        System.out.println(Bytes.toLong(CellUtil.cloneValue(kv)));
        Assert.assertTrue(Bytes.toLong(CellUtil.cloneValue(kv)) < 100);

        Thread.sleep(2000);
        get.setMaxVersions(200);
        result = hTable.get(get);
        assertEquals(101, result.rawCells().length);
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

        Put put = new Put(keyBytes);
        put.addColumn(family.getBytes(), columnBytes, valueBytes);
        hTable.put(put);

        // check delete column
        Delete delete = new Delete(keyBytes);
        delete.addColumn(family.getBytes(), columnBytes);
        boolean ret = hTable.checkAndDelete(keyBytes, family.getBytes(), columnBytes, valueBytes,
            delete);
        Assert.assertTrue(ret);

        // 2. test normal filter with special chracter
        put = new Put(keyBytes);
        put.addColumn(family.getBytes(), columnBytes, valueBytes);
        hTable.put(put);

        Get get = new Get(keyBytes);
        get.addFamily(family.getBytes());
        // 2.1 test special row
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            keyBytes));
        get.setFilter(rowFilter);
        Result result = hTable.get(get);
        Assert.assertEquals(1, result.rawCells().length);
        Assert.assertArrayEquals(keyBytes, result.getRow());

        // 2.2 test special column
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
            family.getBytes(), columnBytes, CompareFilter.CompareOp.EQUAL, valueBytes);
        get.setFilter(singleColumnValueFilter);
        result = hTable.get(get);
        Assert.assertEquals(1, result.rawCells().length);
        Assert.assertArrayEquals(keyBytes, result.getRow());

        // 2.3 test special value
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(valueBytes));
        get.setFilter(valueFilter);
        result = hTable.get(get);
        Assert.assertEquals(1, result.rawCells().length);
        Assert.assertArrayEquals(keyBytes, result.getRow());

    }

    private static String generateRandomStringByUUID(int times) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < times; i++) {
            sb.append(UUID.randomUUID().toString().replaceAll("-", ""));
        }
        return sb.toString();
    }
}
