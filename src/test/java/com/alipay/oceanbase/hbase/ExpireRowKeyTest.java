package com.alipay.oceanbase.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.jruby.RubyProcess;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class ExpireRowKeyTest {
    OHTableClient hTable;
    //// 需要测试 forward_scan TTL/MaxVersions 过期 rowKey 的采集
    //// 针对过期的 qualifier c1 进行讨论，有如下几种情况：
    // 1. c1 指定结果列中
    //  1.1. c1 是非最后一个 qualifier
    //  1.2. c1 是最后一个 qualifier
    // 2. c1 未指定在查询列中，但是中间会跳过 c1

    // NOTE: reverse scan 当前客户端不支持，暂时不需要测试

    @Before
    public void setup() throws Exception {
        hTable = ObHTableTestUtil.newOHTableClient("test");
        hTable.init();
    }

    void executeScan(byte[] familyName, byte[] startRow, byte[] endRow) throws Exception
    {
        Scan scan = new Scan();
        scan.addFamily(familyName);
        scan.setStartRow(startRow);
        scan.setStopRow(endRow);
        scan.setMaxVersions(1);
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {}
    }

    void executeScan(byte[] familyName, byte[] startRow, byte[] endRow, byte[] col) throws Exception
    {
        Scan scan = new Scan();
        scan.addColumn(familyName, col);
        scan.setStartRow(startRow);
        scan.setStopRow(endRow);
        scan.setMaxVersions(1);
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {}
    }

    void executeScan(byte[] familyName, byte[] startRow, byte[] endRow, byte[][] cols) throws Exception
    {
        Scan scan = new Scan();
        for (int i = 0; i < cols.length; i++) {
            scan.addColumn(familyName, cols[i]);
        }
        scan.setStartRow(startRow);
        scan.setStopRow(endRow);
        scan.setMaxVersions(1);
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {}
    }


    /*
        CREATE TABLE `test$family_expired_rowkey` (
            `K` varbinary(1024) NOT NULL,
            `Q` varbinary(256) NOT NULL,
            `T` bigint(20) NOT NULL,
            `V` varbinary(1024) DEFAULT NULL,
        PRIMARY KEY (`K`, `Q`, `T`)) kv_attributes ='{"Hbase": {"MaxVersions": 2, "TimeToLive": 3600}}';
    */
    @Test
    public void testExpiredRowKey() throws Exception {
        byte[] familyName = "family_expired_rowkey".getBytes();
        byte[] row0 = "row0".getBytes();
        byte[] row1 = "row1".getBytes();
        byte[] row2 = "row2".getBytes();
        byte[] row3 = "row3".getBytes();
        byte[] row4 = "row4".getBytes();
        byte[] row5 = "row5".getBytes();
        byte[] row6 = "row6".getBytes();
        byte[] row7 = "row7".getBytes();
        byte[] row8 = "row8".getBytes();
        byte[] row9 = "row9".getBytes();
        byte[] row10 = "row10".getBytes();
        byte[] row11 = "row11".getBytes();
        byte[] row12 = "row12".getBytes();
        byte[] row13 = "row13".getBytes();
        byte[] row14 = "row14".getBytes();
        byte[] row15 = "row15".getBytes();
        byte[] row16 = "row16".getBytes();

        byte[][] maxVersionRows = {row0, row1, row2, row3, row4, row5, row6, row7};
        byte[][] ttlRows = {row8, row9, row10, row11, row12, row13, row14, row15};

        byte[] col0 = "c0".getBytes();
        byte[] col1 = "c1".getBytes();
        byte[] col2 = "c2".getBytes();
        byte[][] cols = {col0, col1, col2};

        byte[] value = "value".getBytes();
        int[][] rowVersions = {
                {2, 3, 2}, // row0
                {2, 3, 2}, // row1
                {2, 3, 2}, // row2
                {2, 3, 2}, // row3
                {2, 3, 2}, // row4
                {2, 3, 2}, // row5
                {2, 3, 2}, // row6
                {2, 2, 2}, // row7, not expire
        };


        // every qualifier has two version, the value 1 means the old version is expired by timetotive
        int[][] expireFlags = {
                {0, 1, 0}, // row8
                {0, 1, 0}, // row9
                {0, 1, 0}, // row10
                {0, 1, 0}, // row11
                {0, 1, 0}, // row12
                {0, 1, 0}, // row13
                {0, 1, 0}, // row14
                {0, 0, 0}, // row15, not expire
        };

        try {
            for (int i = 0; i < maxVersionRows.length; i++) {
                for (int j = 0; j < cols.length; j++) {
                    for (int k = 0; k < rowVersions[i][j]; k++) {
                        Put put = new Put(maxVersionRows[i]);
                        put.add(familyName, cols[j], value);
                        hTable.put(put);
                    }
                }
            }

            for (int i = 0; i < ttlRows.length; i++) {
                for (int j = 0; j < cols.length; j++) {
                    if (expireFlags[i][j] > 0) {
                        Put put = new Put(ttlRows[i]);
                        put.add(familyName, cols[j], 0, value);
                        hTable.put(put);
                    } else {
                        Put put = new Put(ttlRows[i]);
                        put.add(familyName, cols[j], value);
                        hTable.put(put);
                    }
                    Put put = new Put(ttlRows[i]);
                    put.add(familyName, cols[j], value);
                    hTable.put(put);
                }
            }

            // 1. scan all qualifier for row0
            // will record row0
            executeScan(familyName, row0, row1);

            // 2. scan c1 qualifier for row2
            // will record row2
            executeScan(familyName, row2, row3, col1);

            // 3. scan c0, c2 qualifier for row4
            // will record row4
            executeScan(familyName, row4, row5, new byte[][]{col0, col2});

            // 3. scan c0 qualifier from row6 to row7
            // will record row6
            executeScan(familyName, row6, row8, col0);

            // 4. scan all qualifier for row8
            // will record row8
            executeScan(familyName, row8, row9);

            // 5. scan c1 qualifier for row10
            // will record row10
            executeScan(familyName, row10, row11, col1);

            // 6. scan c0, c2 qualifier for row12
            // will record row12
            executeScan(familyName, row12, row13, new byte[][]{col0, col2});

            // 7. scan c0 qualifier from row14 to row16
            // will record row14
            executeScan(familyName, row14, row16, col0);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            /*
            for (int i = 0; i < maxVersionRows.length; i++) {
                for (int j = 0; j < cols.length; j++) {
                  for (int k = 0; k < rowVersions[i][j]; k++) {
                      Delete del = new Delete(maxVersionRows[i]);
                      del.deleteColumn(familyName, cols[j]);
                      hTable.delete(del);
                  }
                }
            }
             */
        }
    }
}
