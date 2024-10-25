package com.alipay.oceanbase.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHTableDeleteFamilyVersionTest {
    @Rule
    public ExpectedException    expectedException = ExpectedException.none();

    protected Table             hTable;
    private static final String key1              = "scanKey1x";
    private static final String key2              = "scanKey2x";
    private static final String key3              = "scanKey3x";
    private static final String column1           = "column1";
    private static final String column2           = "column2";
    private static final String column3           = "column3";
    private static final String value1            = "value1";
    private static final String value2            = "value2";
    private static final String value3            = "value3";
    private static final String family1           = "family_with_group1";
    private static final String family2           = "family_with_group2";

    @Before
    public void before() throws Exception {
        hTable = ObHTableTestUtil.newOHTableClient("test_multi_cf");
        ((OHTableClient) hTable).init();
    }

    @After
    public void finish() throws IOException {
        hTable.close();
    }

    public void tryPut(Table hTable, Put put) throws Exception {
        hTable.put(put);
        Thread.sleep(1);
    }

    @Test
    public void testDeleteFamilyVerison() throws Exception {
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

        Put putKey3Fam1Column2Ts6 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts6.addColumn(toBytes(family1), toBytes(column2), timeStamp6,
            toBytes(value1));

        Put putKey2Fam1Column3Ts6 = new Put(toBytes(key2));
        putKey2Fam1Column3Ts6.addColumn(toBytes(family1), toBytes(column3), timeStamp3,
            toBytes(value1));

        tryPut(hTable, putKey1Fam1Column1MinTs);
        tryPut(hTable, putKey3Fam1Column1Ts1);
        tryPut(hTable, putKey1Fam1Column2MinTs);
        tryPut(hTable, putKey1Fam1Column2Ts3);
        tryPut(hTable, putKey2Fam1Column2Ts3);
        tryPut(hTable, putKey2Fam1Column3Ts1);
        tryPut(hTable, putKey3Fam1Column3Ts1);
        tryPut(hTable, putKey3Fam1Column2Ts6);
        tryPut(hTable, putKey2Fam1Column3Ts6);

        // test DeleteFamilyVersion single cf
        Get get = new Get(toBytes(key1));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(minTimeStamp);
        get.setMaxVersions(10);
        Result r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        get = new Get(toBytes(key3));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(timeStamp1);
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        get = new Get(toBytes(key2));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(timeStamp3);
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        Delete delKey1MinTs = new Delete(toBytes(key1));
        delKey1MinTs.addFamilyVersion(toBytes(family1), minTimeStamp);
        hTable.delete(delKey1MinTs);

        get = new Get(toBytes(key1));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(minTimeStamp);
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        Delete delKey3Ts1 = new Delete(toBytes(key3));
        delKey3Ts1.addFamilyVersion(toBytes(family1), timeStamp1);
        hTable.delete(delKey3Ts1);

        get = new Get(toBytes(key3));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(timeStamp1);
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        Delete delKey2Ts3 = new Delete(toBytes(key2));
        delKey2Ts3.addFamilyVersion(family1.getBytes(), timeStamp3);
        hTable.delete(delKey2Ts3);

        get = new Get(toBytes(key2));
        get.addFamily(toBytes(family1));
        get.setTimeStamp(timeStamp3);
        get.setMaxVersions(10);
        r = hTable.get(get);
        Assert.assertEquals(0, r.rawCells().length);

        Scan scan = new Scan();
        scan.setStartRow(toBytes(key1));
        scan.setStopRow("scanKey4x".getBytes());
        scan.addFamily(toBytes(family1));
        scan.setMaxVersions(10);
        ResultScanner scanner = hTable.getScanner(scan);
        int key1Cnt = 0, key2Cnt = 0, key3Cnt = 0;
        for (Result result : scanner) {
            for (Cell kv : result.rawCells()) {
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

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);

        // test DeleteFamilyVersion multiple cf
        Put putKey1Fam1Column3Ts6 = new Put(toBytes(key1));
        putKey1Fam1Column3Ts6.addColumn(toBytes(family1), toBytes(column3), timeStamp6,
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

        Put putKey2Fam1Column2Ts8 = new Put(toBytes(key2));
        putKey2Fam1Column2Ts8.addColumn(toBytes(family1), toBytes(column2), timeStamp8,
            toBytes(value2));

        Put putKey2Fam2Column3Ts1 = new Put(toBytes(key2));
        putKey2Fam2Column3Ts1.addColumn(toBytes(family2), toBytes(column3), timeStamp3,
            toBytes(value3));

        Put putKey2Fam1Column1Ts1 = new Put(toBytes(key2));
        putKey2Fam1Column1Ts1.addColumn(toBytes(family1), toBytes(column1), timeStamp8,
            toBytes(value1));

        Put putKey2Fam2Column1Ts3 = new Put(toBytes(key2));
        putKey2Fam2Column1Ts3.addColumn(toBytes(family2), toBytes(column1), timeStamp3,
            toBytes(value2));

        Put putKey3Fam1Column2Ts9 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts9.addColumn(toBytes(family1), toBytes(column2), timeStamp9,
            toBytes(value2));

        Put putKey3Fam2Column3Ts10 = new Put(toBytes(key3));
        putKey3Fam2Column3Ts10.addColumn(toBytes(family2), toBytes(column3), timeStamp10,
            toBytes(value1));

        Put putKey3Fam2Column1Ts10 = new Put(toBytes(key3));
        putKey3Fam2Column1Ts10.addColumn(toBytes(family2), toBytes(column1), timeStamp10,
            toBytes(value2));

        Put putKey3Fam1Column2Ts2 = new Put(toBytes(key3));
        putKey3Fam1Column2Ts2.addColumn(toBytes(family1), toBytes(column2), timeStamp2,
            toBytes(value1));

        tryPut(hTable, putKey1Fam1Column3Ts6);
        tryPut(hTable, putKey1Fam2Column2Ts2);
        tryPut(hTable, putKey1Fam2Column3Ts2);
        tryPut(hTable, putKey1Fam1Column2Ts1);
        tryPut(hTable, putKey2Fam1Column2Ts8);
        tryPut(hTable, putKey2Fam2Column3Ts1);
        tryPut(hTable, putKey2Fam1Column1Ts1);
        tryPut(hTable, putKey2Fam2Column1Ts3);
        tryPut(hTable, putKey3Fam1Column2Ts9);
        tryPut(hTable, putKey3Fam2Column3Ts10);
        tryPut(hTable, putKey3Fam2Column1Ts10);
        tryPut(hTable, putKey3Fam1Column2Ts2);

        Get getKey1 = new Get(toBytes(key1));
        getKey1.addFamily(toBytes(family1));
        getKey1.addFamily(toBytes(family2));
        getKey1.setMaxVersions(10);
        r = hTable.get(getKey1);
        Assert.assertEquals(4, r.rawCells().length);

        Get getKey2 = new Get(toBytes(key2));
        getKey2.addFamily(toBytes(family1));
        getKey2.addFamily(toBytes(family2));
        getKey2.setMaxVersions(10);
        r = hTable.get(getKey2);
        Assert.assertEquals(4, r.rawCells().length);

        Get getKey3 = new Get(toBytes(key3));
        getKey3.addFamily(toBytes(family1));
        getKey3.addFamily(toBytes(family2));
        getKey3.setMaxVersions(10);
        r = hTable.get(getKey3);
        Assert.assertEquals(4, r.rawCells().length);

        Delete delKey1Ts_6_2 = new Delete(toBytes(key1));
        delKey1Ts_6_2.addFamilyVersion(toBytes(family1), timeStamp6);
        delKey1Ts_6_2.addFamilyVersion(toBytes(family2), timeStamp2);
        hTable.delete(delKey1Ts_6_2);

        getKey1 = new Get(toBytes(key1));
        getKey1.addFamily(toBytes(family1));
        getKey1.addFamily(toBytes(family2));
        getKey1.setMaxVersions(10);
        r = hTable.get(getKey1);
        Assert.assertEquals(1, r.rawCells().length);
        for (Cell kv : r.rawCells()) {
            Assert.assertEquals(timeStamp1, kv.getTimestamp());
        }

        Delete delKey2Ts_8_3 = new Delete(toBytes(key2));
        delKey2Ts_8_3.addFamilyVersion(toBytes(family1), timeStamp8);
        delKey2Ts_8_3.addFamilyVersion(toBytes(family2), timeStamp3);
        hTable.delete(delKey2Ts_8_3);

        getKey2 = new Get(toBytes(key2));
        getKey2.addFamily(toBytes(family1));
        getKey2.addFamily(toBytes(family2));
        getKey2.setMaxVersions(10);
        r = hTable.get(getKey2);
        Assert.assertEquals(0, r.rawCells().length);

        Delete delKey3Ts_2_10 = new Delete(toBytes(key3));
        delKey3Ts_2_10.addFamilyVersion(toBytes(family1), timeStamp2);
        delKey3Ts_2_10.addFamilyVersion(toBytes(family2), timeStamp10);
        hTable.delete(delKey3Ts_2_10);

        getKey3 = new Get(toBytes(key3));
        getKey3.addFamily(toBytes(family1));
        getKey3.addFamily(toBytes(family2));
        getKey3.setMaxVersions(10);
        r = hTable.get(getKey3);
        Assert.assertEquals(1, r.rawCells().length);
        for (Cell kv : r.rawCells()) {
            Assert.assertEquals(timeStamp9, kv.getTimestamp());
        }

        scan = new Scan();
        scan.setStartRow(toBytes(key1));
        scan.setStopRow("scanKey4x".getBytes());
        scan.addFamily(toBytes(family1));
        scan.addFamily(toBytes(family2));
        scan.setMaxVersions(10);
        scanner = hTable.getScanner(scan);
        int ts1Cnt = 0, ts9Cnt = 0;
        for (Result result : scanner) {
            for (Cell kv : result.rawCells()) {
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
