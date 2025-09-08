/*-
 * #%L
 * OceanBase Table Hbase Framework
 * %%
 * Copyright (C) 2016 - 2021 Ant Financial Services Group
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

import com.alipay.oceanbase.hbase.util.ExecuteAbleManager;
import com.alipay.oceanbase.hbase.util.TableHBaseLoggerFactory;
import com.alipay.sofa.common.log.proxy.LoggerProxy;
import com.mysql.cj.conf.ConnectionUrlParser.Pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PoolMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHTableDDSTest {
    private String        APP_NAME    = "obkv";
    private String        APP_DS_NAME = "obkv4_adapt_dds_client_test";
    private String        VERSION     = "v1.0";

    protected OHTablePool ohTablePool;

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        ohTablePool = new OHTablePool(conf, 10, PoolMap.PoolType.Reusable);
        ohTablePool.setRuntimeBatchExecutor("test", Executors.newFixedThreadPool(33));
        ohTablePool.init();
    }

    /*
     * 
     create table if not exists test$family_01(
        K varbinary(1024) NOT NULL,
        Q varbinary(256) NOT NULL,
        T bigint(20) NOT NULL,
        V varbinary(1024) NOT NULL,
        PRIMARY KEY (K, Q, T)
     ) TABLEGROUP = 'test';
    
     create table if not exists test$family_02(
        K varbinary(1024) NOT NULL,
        Q varbinary(256) NOT NULL,
        T bigint(20) NOT NULL,
        V varbinary(1024) NOT NULL,
        PRIMARY KEY (K, Q, T)
     ) TABLEGROUP = 'test';
     
     create table if not exists test$family_03(
        K varbinary(1024) NOT NULL,
        Q varbinary(256) NOT NULL,
        T bigint(20) NOT NULL,
        V varbinary(1024) NOT NULL,
        PRIMARY KEY (K, Q, T)
     ) TABLEGROUP = 'test';
      
     */

    /**
    * TestCase: 分表路由到单个分区，表为 test$family_00.
    */
    @Test
    public void testSharding00() throws Exception {
        String tableName = "test";
        String family = "family";
        String key1 = "00_TESTKEY0";
        String column1 = "column1";
        String key2 = "00_TESTKEY1";
        String key3 = "00_TESTKEY2";
        String value = "value";
        long timestamp = System.currentTimeMillis();

        HTableInterface hTable = ohTablePool.getTable(tableName);

        try {
            Put put = new Put(toBytes(key1));
            put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(value));
            hTable.put(put);
            Get get = new Get(toBytes(key1));
            get.addColumn(family.getBytes(), toBytes(column1));
            Result r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);

            put = new Put(toBytes(key2));
            put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(value));
            hTable.put(put);

            put = new Put(toBytes(key3));
            put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(value));
            hTable.put(put);

            Scan scan = new Scan();
            scan.addColumn(family.getBytes(), column1.getBytes());
            scan.setStartRow(toBytes(key1));
            scan.setStopRow(toBytes(key3));
            scan.setMaxVersions(9);
            ResultScanner scanner = hTable.getScanner(scan);

            int count = 0;
            for (Result result : scanner) {
                for (KeyValue keyValue : result.raw()) {
                    System.out.println("rowKey: " + new String(keyValue.getRow())
                                       + " columnQualifier:" + new String(keyValue.getQualifier())
                                       + " timestamp:" + keyValue.getTimestamp() + " value:"
                                       + new String(keyValue.getValue()));
                    count++;
                }
            }

            Assert.assertEquals(2, count);
        } finally {
            Delete delete = new Delete(toBytes(key1));
            delete.deleteFamily(family.getBytes());
            hTable.delete(delete);
            delete = new Delete(toBytes(key2));
            delete.deleteFamily(family.getBytes());
            hTable.delete(delete);
            delete = new Delete(toBytes(key3));
            delete.deleteFamily(family.getBytes());
            hTable.delete(delete);
        }
    }

    /**
     * TestCase: 分表路由到多个分区，分表为 test$family_{00-03}.
     */
    @Test
    public void testSharding01() throws Exception {
        String tableName = "test";
        String family = "family";
        String column1 = "column1";
        String key[] = new String[] { "00TEST_KEY0", "01TEST_KEY1", "02TEST_KEY2", "03TEST_KEY3" };
        String value = "value";
        long timestamp = System.currentTimeMillis();

        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        HTableInterface hTable = new OHTable(conf, tableName);

        try {
            for (int i = 0; i < key.length; i++) {
                Put put = new Put(toBytes(key[i]));
                put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(value));
                hTable.put(put);
                Get get = new Get(toBytes(key[i]));
                get.addColumn(family.getBytes(), toBytes(column1));
                Result r = hTable.get(get);
                Assert.assertEquals(1, r.raw().length);
            }

            // bad partition : 05
            try {
                Put put = new Put(toBytes("05TEST_KEY0"));
                put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(value));
                hTable.put(put);
                Assert.fail("expect exception");
            } catch (Exception e) {
                Assert.assertTrue("unsupported", true);
            }

            Scan scan = new Scan();
            scan.addColumn(family.getBytes(), column1.getBytes());
            scan.setStartRow(toBytes("01TEST_KEY1"));
            scan.setStopRow(toBytes("01TEST_KEY3"));
            scan.setMaxVersions(9);
            ResultScanner scanner = hTable.getScanner(scan);

            int count = 0;
            for (Result result : scanner) {
                for (KeyValue keyValue : result.raw()) {
                    Assert.assertEquals(value, new String(keyValue.getValue()));
                    System.out.println("rowKey: " + new String(keyValue.getRow())
                                       + " columnQualifier:" + new String(keyValue.getQualifier())
                                       + " timestamp:" + keyValue.getTimestamp() + " value:"
                                       + new String(keyValue.getValue()));
                    count++;
                }
            }
            Assert.assertEquals(1, count);

            // 跨 partition 的 Scan 是不支持的
            try {
                scan = new Scan();
                scan.addColumn(family.getBytes(), column1.getBytes());
                scan.setStartRow(toBytes(key[1]));
                scan.setStopRow(toBytes(key[3]));
                scan.setMaxVersions(9);
                hTable.getScanner(scan);
                Assert.fail("expect exception");
            } catch (Exception e) {
                Assert.assertTrue("unsupported", true);
            }
        } finally {
            for (int i = 0; i < key.length; i++) {
                Delete delete = new Delete(toBytes(key[i]));
                delete.deleteFamily(family.getBytes());
                hTable.delete(delete);
            }
        }
    }
    @Test
    public void testAppendIncrement() throws Exception {
        String tableName = "test";
        String family = "family";
        String column1 = "column1";
        String key1 = "00_TESTKEY0";
        String value = "value";
        String appendValue = "appendValue";
        String key2 = "00_TESTKEY1";
        long incrValue = 100L;

        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        HTableInterface hTable = new OHTable(conf, tableName);

        try {
            long timestamp = System.currentTimeMillis();
            Put put = new Put(toBytes(key1));
            put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(value));
            hTable.put(put);

            Get get = new Get(toBytes(key1));
            get.addColumn(family.getBytes(), toBytes(column1));
            Result r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);

            Assert.assertEquals(value, new String(r.getValue(family.getBytes(), column1.getBytes())));

            Append append = new Append(toBytes(key1));
            append.add(family.getBytes(), column1.getBytes(), toBytes(appendValue));
            hTable.append(append);
            r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);
            Assert.assertEquals(value + appendValue, new String(r.getValue(family.getBytes(), column1.getBytes())));
            
            Increment increment = new Increment(toBytes(key2));
            increment.addColumn(family.getBytes(), column1.getBytes(), incrValue);
            hTable.increment(increment);
            get = new Get(toBytes(key2));
            get.addColumn(family.getBytes(), toBytes(column1));
            r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);
            System.out.println(Arrays.toString(r.getValue(family.getBytes(), column1.getBytes())));
            long actualValue = Bytes.toLong(r.getValue(family.getBytes(), column1.getBytes()));
            Assert.assertEquals(incrValue, actualValue);
        } finally {
            Delete delete = new Delete(toBytes(key1));
            delete.deleteFamily(family.getBytes());
            hTable.delete(delete);
            delete = new Delete(toBytes(key2));
            delete.deleteFamily(family.getBytes());
            hTable.delete(delete);
        }
    }
    
    @Test
    public void testCheckAndPut() throws Exception {
        String tableName = "test";
        String family = "family";
        String column1 = "column1";
        String key1 = "00_TESTKEY0";
        String value = "value";
        String newValue = "newValue";
        long timestamp = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        HTableInterface hTable = new OHTable(conf, tableName);
        try {
            Put put = new Put(key1.getBytes());
            put.add(family.getBytes(), column1.getBytes(), value.getBytes());
            hTable.put(put);
            Get get = new Get(key1.getBytes());
            get.addColumn(family.getBytes(), column1.getBytes());
            Result r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);
            put = new Put(key1.getBytes());
            put.add(family.getBytes(), column1.getBytes(), newValue.getBytes());
            boolean ret = hTable.checkAndPut(key1.getBytes(), family.getBytes(), column1.getBytes(), value.getBytes(), put);
            Assert.assertTrue(ret);
            r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);
            Assert.assertEquals(newValue, new String(r.getValue(family.getBytes(), column1.getBytes())));
        } finally {
            Delete delete = new Delete(key1.getBytes());
            delete.deleteFamily(family.getBytes());
            hTable.delete(delete);
        }
    }
    @Test
    public void testCheckAndDelete() throws Exception {
        String tableName = "test";
        String family = "family";
        String column1 = "column1";
        String key1 = "00_TESTKEY0";
        String value = "value";
        String newValue = "newValue";
        long timestamp = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        HTableInterface hTable = new OHTable(conf, tableName);
        
        Put put = new Put(key1.getBytes());
        put.add(family.getBytes(), column1.getBytes(), value.getBytes());
        hTable.put(put);
        Get get = new Get(key1.getBytes());
        get.addColumn(family.getBytes(), column1.getBytes());
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);
        Delete delete = new Delete(key1.getBytes());
        delete.deleteColumn(family.getBytes(), column1.getBytes());
        boolean ret = hTable.checkAndDelete(key1.getBytes(), family.getBytes(), column1.getBytes(), value.getBytes(), delete);
        Assert.assertTrue(ret);
        r = hTable.get(get);
        Assert.assertEquals(0, r.raw().length);
    }
    
    /*
        create tablegroup if not exists test_multicf SHARDING = 'ADAPTIVE';
        create table if not exists test_multicf$family1_00(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 
      
        create table if not exists test_multicf$family1_01(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 

        create table if not exists test_multicf$family1_02(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 

        create table if not exists test_multicf$family1_03(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 

        create table if not exists test_multicf$family2_00(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 

        create table if not exists test_multicf$family2_01(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 

        create table if not exists test_multicf$family2_02(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 

        create table if not exists test_multicf$family2_03(
            K varbinary(1024) NOT NULL,
            Q varbinary(256) NOT NULL,
            T bigint(20) NOT NULL,
            V varbinary(1024) NOT NULL,
            PRIMARY KEY (K, Q, T)
        ) TABLEGROUP = 'test_multicf'; 
     */

    /**
     * TestCase: 多CF功能测试, 每个CF进行分表
     */
    @Test
    public void testSharding02() throws Exception {
        String tableName = "test_multicf";
        String family1 = "family1";
        String family2 = "family2";
        String column1 = "column1";
        String key[] = new String[] { "00TEST_KEY0", "00TEST_KEY1", "00TEST_KEY2", "00TEST_KEY3",
                "01TEST_KEY1", "01TEST_KEY2", "01TEST_KEY3", "01TEST_KEY4", "02TEST_KEY1",
                "02TEST_KEY2", "02TEST_KEY3", "02TEST_KEY4", "03TEST_KEY1", "03TEST_KEY2",
                "03TEST_KEY3", "03TEST_KEY4" };
        String value = "value";

        Configuration conf = new Configuration();
        conf.set(HBASE_OCEANBASE_DDS_APP_NAME, APP_NAME);
        conf.set(HBASE_OCEANBASE_DDS_APP_DS_NAME, APP_DS_NAME);
        conf.set(HBASE_OCEANBASE_DDS_VERSION, VERSION);
        HTableInterface hTable = new OHTable(conf, tableName);

        try {
            for (int i = 0; i < key.length; i++) {
                Put put = new Put(toBytes(key[i]));
                put.addColumn(family1.getBytes(), (column1 + family1).getBytes(), toBytes(value + key[i]
                                                                                    + family1));
                put.addColumn(family2.getBytes(), (column1 + family2).getBytes(), toBytes(value + key[i]
                                                                                    + family2));
                hTable.put(put);
            }

            for (int i = 0; i < key.length; i++) {
                Get get = new Get(toBytes(key[i]));
                get.addFamily(family1.getBytes());
                get.addFamily(family2.getBytes());
                Result result = hTable.get(get);
                Assert.assertEquals(2, result.raw().length);
            }

            List<Pair<byte[], byte[]>> scans = new ArrayList<>();
            scans.add(new Pair<>(toBytes("00A"), toBytes("00Z")));
            scans.add(new Pair<>(toBytes("01A"), toBytes("01Z")));
            scans.add(new Pair<>(toBytes("02A"), toBytes("02Z")));
            scans.add(new Pair<>(toBytes("03A"), toBytes("03Z")));
            // forward scan
            for (Pair<byte[], byte[]> scan : scans) {
                Scan s = new Scan(scan.left, scan.right);
                s.addFamily(family1.getBytes());
                ResultScanner scanner = hTable.getScanner(s);
                for (Result result : scanner) {
                    System.out.println("Family1 Forward Scan result: " + Arrays.toString(result.raw()));
                }
                scanner.close();
                s = new Scan(scan.left, scan.right);
                s.addFamily(family2.getBytes());
                scanner = hTable.getScanner(s);
                for (Result result : scanner) {
                    System.out.println("Family2 Forward Scan result: " + Arrays.toString(result.raw()));
                }
                scanner.close();
            }
            // reverse scan
            for (Pair<byte[], byte[]> scan : scans) {
                Scan s = new Scan(scan.right, scan.left);
                s.addFamily(family1.getBytes());
                s.setReversed(true);
                ResultScanner scanner = hTable.getScanner(s);
                for (Result result : scanner) {
                    System.out.println("Family1 Reverse Scan result: " + Arrays.toString(result.raw()));
                }
                scanner.close();
                s = new Scan(scan.right, scan.left);
                s.addFamily(family2.getBytes());
                s.setReversed(true);
                scanner = hTable.getScanner(s);
                for (Result result : scanner) {
                    System.out.println("Family2 Reverse Scan result: " + Arrays.toString(result.raw()));
                }
                scanner.close();
            }
               
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("testSharding02 failed");
        } finally {
            for (int i = 0; i < key.length; i++) {
                Delete delete = new Delete(toBytes(key[i]));
                delete.addFamily(family1.getBytes());
                delete.addFamily(family2.getBytes());
                hTable.delete(delete);
            }
        }

        hTable.close();
    }
}
