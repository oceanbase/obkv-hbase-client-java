package com.alipay.oceanbase.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;

public class OHTableMiltiColumnFamilyTest extends HTableTestBase {

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
            }
            count++;
        }
        assertEquals(count, rows);
    }

    @Test
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
            }
            assertEquals(2, keyValues.length);
        }




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
            }
            assertEquals(5, keyValues.length);
        }

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
            }
            assertEquals(5, keyValues.length);
        }


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
            }
            assertEquals(4, keyValues.length);
        }
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
        
        Get get = new Get(toBytes("Key1"));
        Result result = hTable.get(get);
        KeyValue[] keyValues = result.raw();
        long timestamp = keyValues[0].getTimestamp();
        for (int i = 1; i < keyValues.length; ++i) {
            assertEquals(timestamp, keyValues[i].getTimestamp());
        }
        assertEquals(6, keyValues.length);

        Get get2 = new Get(toBytes("Key1"));
        get2.addColumn(family1, family1_column1);
        get2.addColumn(family2, family2_column1);
        get2.addColumn(family2, family2_column2);
        Result result2 = hTable.get(get2);
        keyValues = result2.raw();
        timestamp = keyValues[0].getTimestamp();
        for (int i = 1; i < keyValues.length; ++i) {
            assertEquals(timestamp, keyValues[i].getTimestamp());
        }
        assertEquals(3, keyValues.length);
    }

    @Test
    public void testMultiColumnFamilyDelete() throws Exception {
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
        
        Delete delete = new Delete(toBytes("Key1"));
        delete.deleteColumns(family1, family1_column1);
        delete.deleteColumns(family2, family2_column1);;
        hTable.delete(delete);
        Get get = new Get(toBytes("Key1"));
        Result result = hTable.get(get);
        KeyValue[] keyValues = result.raw();
        assertEquals(4, keyValues.length);
        
        delete = new Delete(toBytes("Key2"));
        delete.deleteFamily(family1);
        delete.deleteFamily(family2);
        hTable.delete(delete);
        get = new Get(toBytes("Key2"));
        result = hTable.get(get);
        keyValues = result.raw();
        assertEquals(3, keyValues.length);

        delete = new Delete(toBytes("Key3"));
        hTable.delete(delete);
        get = new Get(toBytes("Key3"));
        result = hTable.get(get);
        keyValues = result.raw();
        assertEquals(0, keyValues.length);
    }
    
}
