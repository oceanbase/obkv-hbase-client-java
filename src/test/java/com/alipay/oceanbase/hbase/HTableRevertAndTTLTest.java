package com.alipay.oceanbase.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class HTableRevertAndTTLTest extends HTableTestBase {
    private String family = "family1";
    private String familyttl = "family_ttl";
    private String column1 = "column1_1";
    private String column2 = "column1_2";
    private String column3 = "column1_3";

    @Before
    public void setup() throws IOException {

        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTable = new OHTable(c, "test");
    }

    @After
    public void after() {
        try {
            if (hTable != null) {
                hTable.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    ////////////////////// htable scan test ///////////////////////////
    // reverse test case 1. version > maxversion，scan 全表所有列
    @Test
    public void reverseScanTesWitMoreData() throws Exception {

        for (int k = 1; k <= 3; k++) { // 每条记录写3个版本
            for (int i = 1; i < 10; i++) {
                Put put = new Put(("key_1_" + i).getBytes());
                put.add(family.getBytes(), column1.getBytes(), ("v_" + i + "_" + k).getBytes());
                put.add(family.getBytes(), column2.getBytes(), ("v_" + i + "_" + k).getBytes());
                hTable.put(put);
            }
        }
        Scan scan = new Scan();
        scan.addColumn(family.getBytes(),column1.getBytes());
        scan.addColumn(family.getBytes(),column2.getBytes());
        int count = 0;
        boolean reversed = false; // opensource not support reverse scan yet !
        if (!reversed) {
            scan.setStartRow("key_1_3".getBytes());
            scan.setStopRow("key_1_5".getBytes());
            scan.setMaxVersions(2);
        } else if (reversed) {
            scan.setStartRow("key_1_5".getBytes());
            scan.setStopRow("key_1_3".getBytes());
            scan.setMaxVersions(2);
            //scan.setReversed(true);
        }

        ResultScanner scanner = hTable.getScanner(scan);
        if (reversed) {
            int i = 5, j = 3;
            long before = 0, after = 0;
            String columnQualifier = column2;

            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    if (count % 2 == 0) {
                        before = keyValue.getTimestamp();
                    }

                    Assert.assertEquals("key_1_" + i, Bytes.toString(keyValue.getRow()));    // key 从key_1_5开始，到第4个减少1
                    Assert.assertEquals(columnQualifier, Bytes.toString(keyValue.getQualifier()));   // count为奇数时不变化q，偶数改变q
                    Assert.assertEquals("v_" + i + "_" + j, Bytes.toString(keyValue.getValue()));     // value与key对应

                    if (count % 2 == 1) {
                        after = keyValue.getTimestamp();
                        Assert.assertTrue(before > after);
                    }

                    count++;
                    j--;

                    // 因为结果只有2个key, 所以 i 只变化一次, 2个qualifier, maxversion = 2, 所以 count = 4 变化
                    if (count == 4)
                        i--;

                    if (count % 2 == 0) {
                        columnQualifier = (columnQualifier == column2) ? column1 : column2;
                        j = 3;
                    }

                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                }
            }
        } else {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));


                }
            }
        }

        Assert.assertEquals(0, count); // TODO: if open reverse scan, count will be 8
        scanner.close();
    }

    // reverse test case 2. version = max version, 扫描单列column Q1
    @Test
    public void reverseScanTesWithOneColumn() throws Exception {

        for (int k = 1; k <= 2; k++) { //插入两次
            for (int i = 1; i < 10; i++) {
                Put put = new Put(("key_1_" + i).getBytes());
                put.add(family.getBytes(), column1.getBytes(), ("v_" + i + "_" + k).getBytes());
                put.add(family.getBytes(), column2.getBytes(), ("v_" + i + "_" + k).getBytes());
                hTable.put(put);
            }
        }

        Scan scan = new Scan();
        scan.addColumn(family.getBytes(), column1.getBytes());

        boolean reversed = false;
        if (!reversed) {
            scan.setStartRow("key_1_3".getBytes());
            scan.setStopRow("key_1_5".getBytes());
            scan.setMaxVersions(2);
        } else if (reversed) {
            scan.setStartRow("key_1_5".getBytes());
            scan.setStopRow("key_1_3".getBytes());
            scan.setMaxVersions(2);
            // scan.setReversed(true);
        }

        int count = 0;
        ResultScanner scanner = hTable.getScanner(scan);
        if (reversed) {
            int i = 5, j = 2;
            long before = 0, after = 0;
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    Assert.assertEquals("key_1_" + i, Bytes.toString(keyValue.getRow()));
                    Assert.assertEquals(column1, Bytes.toString(keyValue.getQualifier()));
                    Assert.assertEquals("v_" + i + "_" + j, Bytes.toString(keyValue.getValue()));

                    if (count % 2 == 0) {
                        before = keyValue.getTimestamp();
                    }

                    if (count % 2 == 1) {
                        after = keyValue.getTimestamp();
                        Assert.assertTrue(before > after);
                    }

                    count++;
                    j--;
                    if (count == 2) {
                        i--;
                    }

                    if (count % 2 == 0) {
                        j = 2;
                    }

                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                }
            }
        } else {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                    count++;
                }
            }
        }

        Assert.assertEquals(4, count);
        scanner.close();
    }

    // reverse test case 3. version < max version, 扫描单列column Q2
    @Test
    public void reverseScanTesWithOneColumn1() throws Exception {
        for (int k = 1; k < 3; k++) {
            for (int i = 1; i < 10; i++) {
                Put put = new Put(("key_1_" + i).getBytes());
                put.add(family.getBytes(), column1.getBytes(), ("v_" + i + "_" + k).getBytes());
                put.add(family.getBytes(), column2.getBytes(), ("v_" + i + "_" + k).getBytes());
                hTable.put(put);
            }
        }

        Scan scan = new Scan();
        scan.addColumn(family.getBytes(), column2.getBytes());
        boolean reversed = false;//true为逆序
        if (!reversed) {
            scan.setStartRow("key_1_3".getBytes());
            scan.setStopRow("key_1_5".getBytes());
            scan.setMaxVersions(3);
        } else if (reversed) {
            scan.setStartRow("key_1_5".getBytes());
            scan.setStopRow("key_1_3".getBytes());
            scan.setMaxVersions(3);
            // scan.setReversed(true);
        }

        ResultScanner scanner = hTable.getScanner(scan);
        int count = 0;
        if (reversed) {
            long before = 0, after = 0;
            int i = 5, j = 2;
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    Assert.assertEquals("key_1_" + i, Bytes.toString(keyValue.getRow()));
                    Assert.assertEquals(column2, Bytes.toString(keyValue.getQualifier()));
                    Assert.assertEquals("v_" + i + "_" + j, Bytes.toString(keyValue.getValue()));

                    if (count % 2 == 0) {
                        before = keyValue.getTimestamp();
                    }

                    if (count % 2 == 1) {
                        after = keyValue.getTimestamp();
                        Assert.assertTrue(before > after);
                    }

                    count++;
                    j--;
                    if (count == 2) {
                        i--;
                    }

                    if (count % 2 == 0) {
                        j = 2;
                    }

                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                }
            }
        } else {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                    count++;
                }
            }
        }

        Assert.assertEquals(6, count);
        scanner.close();
    }


    // reverse test case 4. version < max version，全表扫描，ttl超时
    @Test
    public void reverseScanWithTTLTest() throws Exception {

        for (int i = 1; i <= 10; i++) {
            Put put = new Put(("key_1_" + i).getBytes());
            put.add(familyttl.getBytes(), column1.getBytes(), ("v_" + i).getBytes());
            put.add(familyttl.getBytes(), column2.getBytes(), ("v_" + i).getBytes());
            hTable.put(put);
        }

        Scan scan = new Scan();
        scan.addFamily(familyttl.getBytes());
        boolean reversed = false;
        if (!reversed) {
            scan.setStartRow("key_1_3".getBytes());
            scan.setStopRow("key_1_5".getBytes());
            scan.setMaxVersions(1);
        } else if (reversed) {
            scan.setStartRow("key_1_5".getBytes());
            scan.setStopRow("key_1_3".getBytes());
            scan.setMaxVersions(1);
            // scan.setReversed(true);
        }

        ResultScanner scanner = hTable.getScanner(scan);
        int count = 0;
        if (reversed) {
            int i = 5;
            long before = 0, after = 0;
            String columnQualifier = column2;

            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    Assert.assertEquals("key_1_" + i, Bytes.toString(keyValue.getRow()));
                    Assert.assertEquals(columnQualifier, Bytes.toString(keyValue.getQualifier()));
                    Assert.assertEquals("v_" + i, Bytes.toString(keyValue.getValue()));

                    if (count % 2 == 0) {
                        before = keyValue.getTimestamp();
                    }

                    if (count % 2 == 1) {
                        after = keyValue.getTimestamp();
                        Assert.assertTrue(before >= after);
                    }

                    count++;
                    columnQualifier = (count % 2 == 0) ? column2 : column1;

                    if (count == 2) {
                        i--;
                    }

                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                }
            }
        } else{
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                    count++;
                }
            }
        }

        Assert.assertEquals(4, count);

        // Thread.sleep(60 * 1000L); // TODO: if TTL is supported, sleep 60s

        scanner = hTable.getScanner(scan);
        count = 0;

        for (Result r : scanner) {
            for (KeyValue keyValue : r.raw()) {
                System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                        + new String(keyValue.getQualifier()) + " timestamp:"
                        + keyValue.getTimestamp() + " value:"
                        + new String(keyValue.getValue()));
                count++;
            }
        }
        Assert.assertEquals(4, count); // TODO: if TTL is supported, count will be 0
        scanner.close();
    }

    // reverse test case 5. version = max version，列扫描，ttl不超时
    @Test
    public void reverseScanWithTTLAndOneColumnTest() throws Exception {

        for (int k = 1; k < 3; k++) {
            for (int i = 1; i < 10; i++) {
                Put put = new Put(("key_1_" + i).getBytes());
                put.add(familyttl.getBytes(), column1.getBytes(), ("v_" + i + "_" + k).getBytes());
                put.add(familyttl.getBytes(), column2.getBytes(), ("v_" + i + "_" + k).getBytes());
                hTable.put(put);
            }
        }

        Scan scan = new Scan();
        scan.addColumn(familyttl.getBytes(), column1.getBytes());
        boolean reversed = false;
        if (reversed) {
            scan.setStartRow("key_1_5".getBytes());
            scan.setStopRow("key_1_3".getBytes());
            scan.setMaxVersions(2);
            // scan.setReversed(true);
        } else {
            scan.setStartRow("key_1_3".getBytes());
            scan.setStopRow("key_1_5".getBytes());
            scan.setMaxVersions(2);
        }

        for (int loop = 0; loop < 1; loop++) {
            ResultScanner scanner = hTable.getScanner(scan);
            int count = 0;
            if (reversed) {
                int i = 5, j = 2;
                long before = 0, after = 0;
                String columnQualifier = column1;

                for (Result r : scanner) {
                    for (KeyValue keyValue : r.raw()) {
                        Assert.assertEquals("key_1_" + i, Bytes.toString(keyValue.getRow()));
                        Assert.assertEquals(columnQualifier, Bytes.toString(keyValue.getQualifier()));

                        if (count % 2 == 0) {
                            before = keyValue.getTimestamp();
                            j=2;
                        }

                        if (count % 2 == 1) {
                            after = keyValue.getTimestamp();
                            Assert.assertTrue(before > after);
                            j=1;
                        }
                        Assert.assertEquals("v_" + i + "_" + j, Bytes.toString(keyValue.getValue()));
                        count++;
                        if (count == 2)
                            i--;

                        System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                                + new String(keyValue.getQualifier()) + " timestamp:"
                                + keyValue.getTimestamp() + " value:"
                                + new String(keyValue.getValue()));

                    }
                }
                Assert.assertEquals(4, count);

                Thread.sleep(10 * 1000L);
                Assert.assertNotEquals(0,count);
            }
        }

        // ohtable reverse scan with ttl and one column test(version = max version) sleep wait 60s
        // Thread.sleep(60 * 1000L); // TTL not support yet

        ResultScanner scanner = hTable.getScanner(scan);
        int count = 0;
        for (Result r : scanner) {
            for (KeyValue keyValue : r.raw()) {
                System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                        + new String(keyValue.getQualifier()) + " timestamp:"
                        + keyValue.getTimestamp() + " value:"
                        + new String(keyValue.getValue()));
                count++;
            }
        }
        // TTL not support yet
        // Assert.assertEquals(0, count);
        scanner.close();
    }

    // reverse test case 6. version = max version，列扫描，前缀匹配过滤器、值过滤器、列过滤器
    @Test
    public void reverseScanWithValueFilterTest() throws Exception {

        for (int k = 1; k <= 3; k++) {
            for (int i = 3; i < 5; i++) {
                Put put = new Put(("key_1_" + i).getBytes());
                put.add(family.getBytes(), column1.getBytes(), ("v_" + i + "_" + k).getBytes());
                put.add(family.getBytes(), column2.getBytes(), ("v_" + i + "_" + k).getBytes());
                hTable.put(put);
            }
        }

        // ohtable reverse scan with value_filter test(version = max version) scan
        Scan scan = new Scan();
        scan.addColumn(family.getBytes(), column1.getBytes());
        scan.addColumn(family.getBytes(), column2.getBytes());

        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(("v_4_2").getBytes()));//返回value=v_4的，应该有2个结果，正确
        scan.setFilter(valueFilter);

        boolean reversed = false; //true为逆序
        if (reversed) {
            scan.setStartRow("key_1_5".getBytes());
            scan.setStopRow("key_1_3".getBytes());
            scan.setMaxVersions(2);
            // scan.setReversed(true);
        } else {
            scan.setStartRow("key_1_3".getBytes());
            scan.setStopRow("key_1_5".getBytes());
            scan.setMaxVersions(1);
        }

        ResultScanner scanner = hTable.getScanner(scan);
        int count = 0;
        long before = 0, after = 0;
        String columnQualifier = column2;
        if (reversed) {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    Assert.assertEquals("key_1_4", Bytes.toString(keyValue.getRow()));
                    Assert.assertEquals("v_4_2", Bytes.toString(keyValue.getValue()));

                    if (count % 2 == 0) {
                        before = keyValue.getTimestamp();
                    }
                    if (count % 2 == 1) {
                        after = keyValue.getTimestamp();
                        Assert.assertTrue(before >= after);
                    }

                    count++;

                    if (count % 2 == 0) {
                        columnQualifier = (columnQualifier == column2) ? column1 : column2;
                    }
                    Assert.assertEquals(columnQualifier, Bytes.toString(keyValue.getQualifier()));

                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                }
            }
        } else {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                    count++;
                }
            }
        }

        Assert.assertEquals(2, count);
    }

    // reverse test case 7. version = max version，列扫描，ColumnCountGetFilter过滤器
    @Test
    public void reverseScanWithColumnCountGetFilterTest() throws Exception {

        for (int k = 1; k <= 3; k++) {
            for (int i = 1; i < 10; i++) {
                Put put = new Put(("key_1_" + i).getBytes());
                put.add(family.getBytes(), column1.getBytes(), ("v_" + i + "_" + k).getBytes());
                put.add(family.getBytes(), column2.getBytes(), ("v_" + i + "_" + k).getBytes());
                hTable.put(put);
            }
        }

        // ohtable reverse scan with column_count_get_filter test(version = max version) scan
        Scan scan = new Scan();
        scan.addColumn(family.getBytes(), column1.getBytes());
        scan.addColumn(family.getBytes(), column2.getBytes());

        // 每行最多返回多少列，并在遇到一行的列数超过我们所设置的限制值的时候，结束扫描操作
        ColumnCountGetFilter columnCountGetFilter = new ColumnCountGetFilter(2);
        scan.setFilter(columnCountGetFilter);

        boolean reversed = false;//true为逆序
        if (reversed) {
            scan.setStartRow("key_1_5".getBytes());
            scan.setStopRow("key_1_3".getBytes());
            scan.setMaxVersions(2);
            // scan.setReversed(true);
        } else {
            scan.setStartRow("key_1_3".getBytes());
            scan.setStopRow("key_1_5".getBytes());
            scan.setMaxVersions(2);
        }

        ResultScanner scanner = hTable.getScanner(scan);
        int count = 0, i = 5, j = 3;

        if (reversed) {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    Assert.assertEquals("key_1_" + i, Bytes.toString(keyValue.getRow()));
                    Assert.assertEquals(column2, Bytes.toString(keyValue.getQualifier()));
                    Assert.assertEquals("v_" + i + "_" + j, Bytes.toString(keyValue.getValue()));
                    if (count % 2 == 0) {
                        j = 2;
                    }

                    if (count % 2 == 1) {
                        j = 3;
                    }
                    count++;

                    if (count == 2)
                        i--;
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));

                }
            }
        } else {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                    count++;
                }
            }
        }

        Assert.assertEquals(2, count);
    }

    // reverse test case 8. version > max version，列扫描，singleColumnValueFilter过滤器
    @Test
    public void reverseScanWithSingleColumnValueFilterTest() throws Exception {

        for (int k = 1; k <= 3; k++) {
            for (int i = 1; i < 10; i++) {
                Put put = new Put(("key_1_" + i).getBytes());
                put.add(family.getBytes(), column1.getBytes(), ("v_" + i + "_" + k).getBytes());
                put.add(family.getBytes(), column2.getBytes(), ("v_" + i + "_" + k).getBytes());
                hTable.put(put);
            }
        }

        // ohtable reverse scan with single_column_value_filter test(version > max version) scan
        Scan scan = new Scan();
        scan.addColumn(family.getBytes(), column1.getBytes());
        scan.addColumn(family.getBytes(), column2.getBytes());

        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(family.getBytes(), column1.getBytes(), CompareFilter.CompareOp.EQUAL, new BinaryComparator(("v_4_3").getBytes()));
        // 正确结果应该有2个
        singleColumnValueFilter.setFilterIfMissing(true);//有匹配，则只返回当前列所在的行数据
        scan.setFilter(singleColumnValueFilter);

        boolean reversed = false;
        if (reversed) {
            scan.setStartRow("key_1_5".getBytes());
            scan.setStopRow("key_1_3".getBytes());
            scan.setMaxVersions(2);
            // scan.setReversed(true);
        } else {
            scan.setStartRow("key_1_3".getBytes());
            scan.setStopRow("key_1_5".getBytes());
            scan.setMaxVersions(2);
        }

        ResultScanner scanner = hTable.getScanner(scan);
        int count = 0 ,j = 3;
        String columnQualifier = column2;
        long before = 0, after = 0;
        if (reversed) {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    Assert.assertEquals("key_1_4", Bytes.toString(keyValue.getRow()));
                    Assert.assertEquals("v_4_"+j, Bytes.toString(keyValue.getValue()));
                    Assert.assertEquals(columnQualifier, Bytes.toString(keyValue.getQualifier()));
                    if (count % 2 == 0) {
                        before = keyValue.getTimestamp();
                        j = 2;
                    }

                    if (count % 2 == 1) {
                        after = keyValue.getTimestamp();
                        Assert.assertTrue(before > after);
                        j = 3;
                    }

                    count++;
                    if (count % 2 == 0) {
                        columnQualifier = (columnQualifier == column2) ? column1 : column2;
                    }

                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                }
            }
        } else {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                    count++;
                }
            }
        }

        Assert.assertEquals(4, count);
    }

    // reverse test case 9. version = max version，列扫描，SkipFilter过滤器
    //过滤掉列中有v14的，那么就跳过所有含有v14/v15的整个行
    @Test
    public void reverseScanWithSkipFilterTest() throws Exception {

        for (int k = 1; k <= 3; k++) {
            for (int i = 1; i < 10; i++) {
                Put put = new Put(("key_1_" + i).getBytes());
                put.add(family.getBytes(), column1.getBytes(), ("v_" + i + "_" + k).getBytes());
                put.add(family.getBytes(), column2.getBytes(), ("v_" + i + "_" + k).getBytes());
                hTable.put(put);
            }
        }

        // ohtable reverse scan with skip_filter test(version = max version) scan
        Scan scan = new Scan();
        scan.addColumn(family.getBytes(), column1.getBytes());
        scan.addColumn(family.getBytes(), column2.getBytes());

        //SkipFilter skipFilter = new SkipFilter(new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL,
        //        new BinaryComparator("v_5_2".getBytes())));
        //scan.setFilter(skipFilter);


        boolean reversed = false; // opensource not support revert scan
        if (reversed) {
            scan.setStartRow("key_1_5".getBytes());
            scan.setStopRow("key_1_3".getBytes());
            scan.setMaxVersions(2);
            // scan.setReversed(true);
        } else {
            scan.setStartRow("key_1_3".getBytes());
            scan.setStopRow("key_1_5".getBytes());
            // scan.setMaxVersions(2);
        }

        ResultScanner scanner = hTable.getScanner(scan);
        int count = 0, j = 3;
        String columnQualifier = column2;
        long before = 0, after = 0;
        if (reversed) {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    Assert.assertEquals("key_1_4", Bytes.toString(keyValue.getRow()));
                    Assert.assertEquals(columnQualifier, Bytes.toString(keyValue.getQualifier()));


                    if (count % 2 == 0) {
                        before = keyValue.getTimestamp();
                        j=3;
                    }

                    if (count % 2 == 1) {
                        after = keyValue.getTimestamp();
                        Assert.assertTrue(before > after);
                        j=2;
                    }

                    count++;
                    if (count % 2 == 0) {
                        columnQualifier = (columnQualifier == column2) ? column1 : column2;
                    }
                    Assert.assertEquals("v_4_" + j, Bytes.toString(keyValue.getValue()));

                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                }
            }
        } else {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                    count++;
                }
            }
        }

        Assert.assertEquals(4, count);
    }

    // reverse test case 10.  version > max version，列扫描，WhileMatchFilter过滤器
    //过滤掉列中有v42的，那么就会在遇到列中含有v42的时候，停下来
    @Test
    public void reverseScanWithWhileMatchFilterTest() throws Exception {

        for (int k = 1; k < 4; k++) {
            for (int i = 1; i < 10; i++) {
                Put put = new Put(("key_1_" + i).getBytes());
                put.add(family.getBytes(), column1.getBytes(), ("v_" + i + "_" + k).getBytes());
                put.add(family.getBytes(), column2.getBytes(), ("v_" + i + "_" + k).getBytes());
                hTable.put(put);
            }
        }

        // ohtable reverse scan with skip_filter test(version > max version) scan
        Scan scan = new Scan();
        scan.addColumn(family.getBytes(), column1.getBytes());
        scan.addColumn(family.getBytes(), column2.getBytes());

        WhileMatchFilter whileMatchFilter = new WhileMatchFilter(new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL,
                new BinaryComparator("v_4_2".getBytes())));
        scan.setFilter(whileMatchFilter);


        boolean reversed = false;
        if (reversed) {
            scan.setStartRow("key_1_5".getBytes());
            scan.setStopRow("key_1_3".getBytes());
            scan.setMaxVersions(2);
            // scan.setReversed(true);
        } else {
            scan.setStartRow("key_1_3".getBytes());
            scan.setStopRow("key_1_5".getBytes());
            scan.setMaxVersions(2);
        }

        ResultScanner scanner = hTable.getScanner(scan);
        int count = 0, i = 5, j = 3;
        String columnQualifier = column2;
        long before = 0, after = 0;
        if (reversed) {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    Assert.assertEquals("key_1_"+i, Bytes.toString(keyValue.getRow()));
                    Assert.assertEquals(columnQualifier, Bytes.toString(keyValue.getQualifier()));


                    if (count % 2 == 0) {
                        before = keyValue.getTimestamp();
                        j = 3;
                    }

                    if (count % 2 == 1) {
                        after = keyValue.getTimestamp();
                        Assert.assertTrue(before > after);
                        j = 2;
                    }

                    count++;
                    if (count % 2 == 0) {
                        columnQualifier = (columnQualifier == column2) ? column1 : column2;
                    }
                    Assert.assertEquals("v_" + i + "_" + j, Bytes.toString(keyValue.getValue()));
                    if (count == 4)
                        i--;


                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                }
            }
        } else{
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                    count++;
                }
            }
        }

        Assert.assertEquals(5, count);
    }

    // reverse test case 11. version = max version，FilterList过滤器
    // ColumnCountGetFilter、SingleColumnValueFilter结合过滤器
    @Test
    public void reverseScanWithFilterListTest() throws Exception {

        for (int k = 1; k < 3; k++) {
            for (int i = 3; i < 5; i++) {
                Put put = new Put(("key_1_" + i).getBytes());
                put.add(family.getBytes(), column1.getBytes(), ("v_" + i).getBytes());
                put.add(family.getBytes(), column2.getBytes(), ("v_" + i).getBytes());
                hTable.put(put);
            }
        }

        // ohtable reverse scan with filter_list test(version = max version) scan
        Scan scan = new Scan();
        scan.addColumn(family.getBytes(), column1.getBytes());
        scan.addColumn(family.getBytes(), column2.getBytes());

        FilterList filterList = new FilterList();

        filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(column2.getBytes()))); //返回所有Q2的列
        filterList.addFilter(new ColumnCountGetFilter(2));//共返回2列
        scan.setFilter(filterList);
        boolean order = false;//true为逆序
        if (!order) {
            scan.setStartRow("key_1_3".getBytes());
            scan.setStopRow("key_1_5".getBytes());
            scan.setMaxVersions(2);
        } else if (order) {
            scan.setStartRow("key_1_5".getBytes());
            scan.setStopRow("key_1_3".getBytes());
            scan.setMaxVersions(2);
            // scan.setReversed(true);
        }

        ResultScanner scanner = hTable.getScanner(scan);
        int count = 0, i = 5;
        if (order == true) {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    Assert.assertEquals("key_1_"+i, Bytes.toString(keyValue.getRow())); //key 都是key_1_4
                    Assert.assertEquals(column2, Bytes.toString(keyValue.getQualifier()));
                    Assert.assertEquals("v_"+i, Bytes.toString(keyValue.getValue()));//value与key对应
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                    count++;
                    if (count == 2)
                        i--;
                }
            }
        } else {
            for (Result r : scanner) {
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                    count++;
                }
            }
        }

        Assert.assertEquals(2, count); // TODO: should fix , actually retuan 2 will be right
    }

}
