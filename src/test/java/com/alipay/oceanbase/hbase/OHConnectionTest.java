/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2024 OceanBase Group
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

import com.alibaba.fastjson.JSON;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import java.util.stream.Stream;
import java.util.Optional;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.*;

public class OHConnectionTest {
    protected static Table hTable;
    protected Connection   connection;

    @Test
    public void testConnectionBySet() throws Exception {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL,
                "com.alipay.oceanbase.hbase.util.OHConnectionImpl");
        c.set("rs.list.acquire.read.timeout", "10000");
        // test set rpc connection timeout, the first one is the latest version
        c.set(SOCKET_TIMEOUT_CONNECT, "15000");
        // the second one is the deprecated version
        c.set(SOCKET_TIMEOUT, "12000");
        connection = ConnectionFactory.createConnection(c);
        TableName tableName = TableName.valueOf("test");
        hTable = connection.getTable(tableName);
        testBasic();
        hTable.close();
    }

    @Test
    public void testConnectionByXml() throws Exception {

        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        // can set rpc connection timeout in xml
        connection = ConnectionFactory.createConnection(c);
        TableName tableName = TableName.valueOf("test");
        hTable = connection.getTable(tableName);
        testBasic();
        hTable.close();
    }

    @Test
    public void testRefreshTableEntry() throws Exception {
        hTable = ObHTableTestUtil.newOHTableClient("n1:test");
        ((OHTableClient) hTable).init();
        ((OHTableClient) hTable).refreshTableEntry("family1", false);
        ((OHTableClient) hTable).refreshTableEntry("family1", true);
    }

    @Test
    public void testNew() throws Exception {
        OHTableClient hTable2 = ObHTableTestUtil.newOHTableClient("n1:test");
        hTable2.init();
        hTable2.getConfiguration().set("rs.list.acquire.read.timeout", "10000");

        assertTrue(hTable2.isAutoFlush());
        hTable2.setAutoFlush(false);
        assertFalse(hTable2.isAutoFlush());
        hTable2.setAutoFlush(true, true);
        assertTrue(hTable2.isAutoFlush());
        hTable2.setWriteBufferSize(10000000L);
        assertEquals(10000000L, hTable2.getWriteBufferSize());
        assertEquals("n1:test", hTable2.getTableNameString());
        assertEquals("n1:test", new String(hTable2.getTableName()));
        hTable2.flushCommits();
        hTable2.close();
        assertTrue(true);
    }

    @After
    public void after() throws IOException {
        if (hTable != null) {
            hTable.close();
        }
    }

    private void testBasic() throws Exception {
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
        put.add(family.getBytes(), column1.getBytes(), timestamp, toBytes(value));
        hTable.put(put);
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), toBytes(column1));
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.raw().length);

        for (KeyValue keyValue : r.raw()) {
            Assert.assertEquals(key, Bytes.toString(keyValue.getRow()));
            Assert.assertEquals(column1, Bytes.toString(keyValue.getQualifier()));
            Assert.assertEquals(timestamp, keyValue.getTimestamp());
            Assert.assertEquals(value, Bytes.toString(keyValue.getValue()));
        }

        put = new Put(toBytes(key));
        put.add(family.getBytes(), column1.getBytes(), timestamp + 1, toBytes(value));
        hTable.put(put);
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), toBytes(column1));
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
            Assert.assertEquals(key, Bytes.toString(keyValue.getRow()));
            Assert.assertEquals(column1, Bytes.toString(keyValue.getQualifier()));
            Assert.assertEquals(timestamp + 1, keyValue.getTimestamp());
            Assert.assertEquals(value, Bytes.toString(keyValue.getValue()));
        }

        try {
            for (int j = 0; j < 10; j++) {
                put = new Put((key + "_" + j).getBytes());
                put.add(family.getBytes(), column1.getBytes(), timestamp + 2, toBytes(value));
                put.add(family.getBytes(), column2.getBytes(), timestamp + 2, toBytes(value));
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
                for (KeyValue keyValue : result.raw()) {
                    Assert.assertEquals(key + "_" + i, Bytes.toString(keyValue.getRow()));
                    Assert.assertTrue(column1.equals(Bytes.toString(keyValue.getQualifier()))
                                      || column2.equals(Bytes.toString(keyValue.getQualifier())));
                    Assert.assertEquals(timestamp + 2, keyValue.getTimestamp());
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
            scan.setMaxVersions(9);
            scanner = hTable.getScanner(scan);
            i = 0;
            count = 0;
            for (Result result : scanner) {
                boolean countAdd = true;
                for (KeyValue keyValue : result.raw()) {
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
    @Test
    public void testBufferedMutatorWithFlush() throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        conf.set("rs.list.acquire.read.timeout", "10000");
        conf.set(SOCKET_TIMEOUT_CONNECT, "15000");
        BufferedMutator bufferMutator = null;
        String key = "putKey";
        String column1 = "putColumn1";
        String value = "value333444";
        String family = "family_group";
        try {
            TableName tableName = TableName.valueOf("test");
            connection = ConnectionFactory.createConnection(conf);
            hTable = connection.getTable(tableName);

            Delete delete= new Delete(toBytes(key));
            delete.deleteFamily(toBytes(family));
            hTable.delete(delete);

            // use defualt params
            bufferMutator = connection.getBufferedMutator(tableName);


            long timestamp = System.currentTimeMillis();

            // only support Put and Delete
            Append append = new Append(Bytes.toBytes(key));
            append.add("family_group".getBytes(), column1.getBytes(), toBytes("_suffix"));
            final BufferedMutator apMut = bufferMutator;
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                apMut.mutate(append);
            });

            List<Mutation> mutations = new ArrayList<>();
            // test Put
            Put put1 = new Put(Bytes.toBytes(key));
            put1.addColumn(Bytes.toBytes(family), Bytes.toBytes(column1), timestamp, Bytes.toBytes(value));
            mutations.add(put1);
            Put put2 = new Put(Bytes.toBytes(key));
            put2.addColumn(Bytes.toBytes(family), Bytes.toBytes(column1 + "1"), timestamp, Bytes.toBytes(value + "4"));
            mutations.add(put2);
            // test add Mutations with List
            bufferMutator.mutate(mutations);
            bufferMutator.flush();

            Get get = new Get(toBytes(key));
            Result r = hTable.get(get);
            Assert.assertEquals(2, r.raw().length);

            Put put3 = new Put(Bytes.toBytes(key));
            final BufferedMutator noCfMut = bufferMutator;
            // test Put without setting family
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                noCfMut.mutate(put3);
            });
            put3.addColumn(Bytes.toBytes(family), Bytes.toBytes(column1 + "2"), timestamp, Bytes.toBytes(value));
            // test add Mutation directly
            bufferMutator.mutate(put3);
            bufferMutator.flush();
            r = hTable.get(get);
            Assert.assertEquals(3, r.raw().length);

            // test Delete
            Delete del = new Delete(toBytes(key));
            del.deleteFamily(toBytes(family));
            bufferMutator.mutate(del);
            bufferMutator.flush();

            r = hTable.get(get);
            Assert.assertEquals(0, r.raw().length);

            // test hybrid mutations
            mutations.clear();
            mutations.add(put1);
            mutations.add(put2);
            mutations.add(del);
            mutations.add(put3);
            bufferMutator.mutate(mutations);
            bufferMutator.flush();

            r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);
        } catch (Exception ex) {
            if (ex instanceof RetriesExhaustedWithDetailsException) {
                ((RetriesExhaustedWithDetailsException) ex).getCauses().get(0).printStackTrace();
            } else {
                ex.printStackTrace();
            }
            Assert.assertTrue(false);
        } finally {
            if (bufferMutator != null ) {
                bufferMutator.close();
                // test flush after closed
                bufferMutator.flush();
                // test add mutations after closed
                Delete delete = new Delete(Bytes.toBytes(key));
                delete.deleteFamily(Bytes.toBytes(family));
                final BufferedMutator closedMutator = bufferMutator;
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.mutate(delete);
                });
            }
        }
    }

    /*
    USE n1;
    CREATE TABLEGROUP `n1:test` SHARDING = 'ADAPTIVE';
    CREATE TABLE `n1:test$family_group` (
                  `K` varbinary(1024) NOT NULL,
                  `Q` varbinary(256) NOT NULL,
                  `T` bigint(20) NOT NULL,
                  `V` varbinary(1024) DEFAULT NULL,
                  PRIMARY KEY (`K`, `Q`, `T`)
            ) TABLEGROUP = `n1:test`;
     */
    @Test
    public void testBufferedMutatorUseNameSpaceWithFlush() throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        conf.set("rs.list.acquire.read.timeout", "10000");
        conf.set(SOCKET_TIMEOUT_CONNECT, "15000");
        BufferedMutator bufferMutator = null;
        String key = "putKey";
        String column1 = "putColumn1";
        String value = "value333444";
        String family = "family_group";
        try {
            // use n1 database
            TableName tableName = TableName.valueOf("n1","test");
            connection = ConnectionFactory.createConnection(conf);
            hTable = connection.getTable(tableName);
            Assert.assertEquals("n1:test", Bytes.toString(((OHTable) hTable).getTableName()));

            Delete delete= new Delete(toBytes(key));
            delete.deleteFamily(toBytes(family));
            hTable.delete(delete);

            // use defualt params
            bufferMutator = connection.getBufferedMutator(tableName);

            long timestamp = System.currentTimeMillis();

            // only support Put and Delete
            Append append = new Append(Bytes.toBytes(key));
            append.add("family_group".getBytes(), column1.getBytes(), toBytes("_suffix"));
            final BufferedMutator apMut = bufferMutator;
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                apMut.mutate(append);
            });

            List<Mutation> mutations = new ArrayList<>();
            // test Put
            Put put1 = new Put(Bytes.toBytes(key));
            put1.addColumn(Bytes.toBytes(family), Bytes.toBytes(column1), timestamp, Bytes.toBytes(value));
            mutations.add(put1);
            Put put2 = new Put(Bytes.toBytes(key));
            put2.addColumn(Bytes.toBytes(family), Bytes.toBytes(column1 + "1"), timestamp, Bytes.toBytes(value + "4"));
            mutations.add(put2);
            // test add Mutations with List
            bufferMutator.mutate(mutations);
            bufferMutator.flush();

            Get get = new Get(toBytes(key));
            Result r = hTable.get(get);
            Assert.assertEquals(2, r.raw().length);

            Put put3 = new Put(Bytes.toBytes(key));
            final BufferedMutator noCfMut = bufferMutator;
            // test Put without setting family
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                noCfMut.mutate(put3);
            });
            put3.addColumn(Bytes.toBytes(family), Bytes.toBytes(column1 + "2"), timestamp, Bytes.toBytes(value));
            // test add Mutation directly
            bufferMutator.mutate(put3);
            bufferMutator.flush();
            r = hTable.get(get);
            Assert.assertEquals(3, r.raw().length);

            // test Delete
            Delete del = new Delete(toBytes(key));
            del.deleteFamily(toBytes(family));
            bufferMutator.mutate(del);
            bufferMutator.flush();

            r = hTable.get(get);
            Assert.assertEquals(0, r.raw().length);

            // test hybrid mutations
            mutations.clear();
            mutations.add(put1);
            mutations.add(put2);
            mutations.add(del);
            mutations.add(put3);
            bufferMutator.mutate(mutations);
            bufferMutator.flush();

            r = hTable.get(get);
            Assert.assertEquals(1, r.raw().length);
        } catch (Exception ex) {
            if (ex instanceof RetriesExhaustedWithDetailsException) {
                ((RetriesExhaustedWithDetailsException) ex).getCauses().get(0).printStackTrace();
            } else {
                ex.printStackTrace();
            }
            Assert.assertTrue(false);
        } finally {
            if (bufferMutator != null ) {
                bufferMutator.close();
                // test flush after closed
                bufferMutator.flush();
                // test add mutations after closed
                Delete delete = new Delete(Bytes.toBytes(key));
                delete.deleteFamily(Bytes.toBytes(family));
                final BufferedMutator closedMutator = bufferMutator;
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.mutate(delete);
                });
            }
        }
    }

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
    @Test
    public void testBufferedMutatorWithAutoFlush() throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        conf.set("rs.list.acquire.read.timeout", "10000");
        BufferedMutator bufferMutator = null;
        BufferedMutatorParams params = null;
        long bufferSize = 45000L;
        int count = 0;
        String key = "putKey";
        String column1 = "putColumn1";
        String value = "value333444";
        long timestamp = System.currentTimeMillis();
        String family = "family_group";
        try {
            TableName tableName = TableName.valueOf("test");
            connection = ConnectionFactory.createConnection(conf);
            hTable = connection.getTable(tableName);

            Delete delete= new Delete(toBytes(key));
            delete.deleteFamily(toBytes(family));
            hTable.delete(delete);

            // set params
            params = new BufferedMutatorParams(tableName);
            params.writeBufferSize(bufferSize);
            bufferMutator = connection.getBufferedMutator(params);

            List<Mutation> mutations = new ArrayList<>();
            for (int i = 0; i < 50; ++i) {
                mutations.clear();
                for (int j = 0; j < 4; ++j) {
                    Put put = new Put(Bytes.toBytes(key));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column1 + "_" + i + "_" + j),
                            timestamp, Bytes.toBytes(value + "_" + i + "_" + j));
                    mutations.add(put);
                }
                if (i % 10 == 0) {  // 0, 10, 20, 30, 40
                    for(int j = 0; j < 4; ++j) {
                        Delete del = new Delete(Bytes.toBytes(key));
                        del.addColumns(toBytes(family), toBytes(column1 + "_" + i + "_" + j));
                        mutations.add(del);
                    }
                }
                bufferMutator.mutate(mutations);
            }
            Get get = new Get(toBytes(key));
            get.addFamily(toBytes(family));
            Result r = hTable.get(get);
            count = r.raw().length;
            Assert.assertTrue(count > 0);
        } catch (Exception ex) {
            if (ex instanceof RetriesExhaustedWithDetailsException) {
                ((RetriesExhaustedWithDetailsException) ex).getCauses().get(0).printStackTrace();
            } else {
                ex.printStackTrace();
            }
            Assert.assertTrue(false);
        } finally {
            if (bufferMutator != null) {
                bufferMutator.close();
                Get get = new Get(toBytes(key));
                get.addFamily(toBytes(family));
                Result r = hTable.get(get);
                count = r.raw().length;
                Assert.assertEquals(180, count);
                Delete delete = new Delete(toBytes(key));
                delete.deleteFamily(toBytes(family));
                hTable.delete(delete);
                r = hTable.get(get);
                Assert.assertEquals(0, r.raw().length);

                // test add mutations after closed
                final BufferedMutator closedMutator = bufferMutator;
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.mutate(delete);
                });
                // test flush after closed
                bufferMutator.flush();
            }

            if (params != null) {
                if (params.getPool() != null) {
                    Assert.assertTrue(params.getPool().isShutdown());
                }
            }
        }
    }

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
    @Test
    public void testBufferedMutatorWithUserPool() throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        conf.set("rs.list.acquire.read.timeout", "10000");
        BufferedMutator ohBufferMutator = null;
        BufferedMutatorParams params = null;
        long bufferSize = 45000L;
        int count = 0;
        String key = "putKey";
        String column1 = "putColumn1";
        String value = "value333444";
        long timestamp = System.currentTimeMillis();
        String family = "family_group";
        try {
            TableName tableName = TableName.valueOf("test");
            connection = ConnectionFactory.createConnection(conf);
            hTable = connection.getTable(tableName);

            Delete delete= new Delete(toBytes(key));
            delete.deleteFamily(toBytes(family));
            hTable.delete(delete);

            // set params
            params = new BufferedMutatorParams(tableName);
            params.writeBufferSize(bufferSize);

            // set thread pool
            long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
            ThreadPoolExecutor pool = new ThreadPoolExecutor(10, 256, keepAliveTime, TimeUnit.SECONDS, new SynchronousQueue(), Threads.newDaemonThreadFactory("htable"));
            pool.allowCoreThreadTimeOut(true);
            params.pool(pool);

            ohBufferMutator = connection.getBufferedMutator(params);

            List<Mutation> mutations = new ArrayList<>();
            for (int i = 0; i < 50; ++i) {
                mutations.clear();
                for (int j = 0; j < 4; ++j) {
                    Put put = new Put(Bytes.toBytes(key));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column1 + "_" + i + "_" + j),
                            timestamp, Bytes.toBytes(value + "_" + i + "_" + j));
                    mutations.add(put);
                }
                if (i % 10 == 0) {  // 0, 10, 20, 30, 40
                    for(int j = 0; j < 4; ++j) {
                        Delete del = new Delete(Bytes.toBytes(key));
                        del.addColumns(toBytes(family), toBytes(column1 + "_" + i + "_" + j));
                        mutations.add(del);
                    }
                }
                ohBufferMutator.mutate(mutations);
            }
            Get get = new Get(toBytes(key));
            get.addFamily(toBytes(family));
            Result r = hTable.get(get);
            count = r.raw().length;
            Assert.assertTrue(count > 0);
        } catch (Exception ex) {
            if (ex instanceof RetriesExhaustedWithDetailsException) {
                ((RetriesExhaustedWithDetailsException) ex).getCauses().get(0).printStackTrace();
            } else {
                ex.printStackTrace();
            }
            Assert.assertTrue(false);
        } finally {
            if (ohBufferMutator != null) {
                ohBufferMutator.close();
                Get get = new Get(toBytes(key));
                get.addFamily(toBytes(family));
                Result r = hTable.get(get);
                count = r.raw().length;
                Assert.assertEquals(180, count);
                Delete delete = new Delete(toBytes(key));
                delete.deleteFamily(toBytes(family));
                hTable.delete(delete);

                r = hTable.get(get);
                Assert.assertEquals(0, r.raw().length);
                // test add mutations after closed
                final BufferedMutator closedMutator = ohBufferMutator;
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.mutate(delete);
                });
                // test flush after closed
                ohBufferMutator.flush();
            }
            if (params != null) {
                if (params.getPool() != null) {
                    Assert.assertTrue(params.getPool().isShutdown());
                }
            }
        }
    }

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
    @Test
    public void testBufferedMutatorConcurrent() throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        conf.set("rs.list.acquire.read.timeout", "10000");
        BufferedMutator ohBufferMutator = null;
        BufferedMutatorParams params = null;
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        long bufferSize = 45000L;
        int count = 0;
        String key = "putKey";
        String column1 = "putColumn1";
        String value = "value333444";
        long timestamp = System.currentTimeMillis();
        String family = "family_group";
        try {
            TableName tableName = TableName.valueOf("test");
            connection = ConnectionFactory.createConnection(conf);
            hTable = connection.getTable(tableName);

            Delete delete= new Delete(toBytes(key));
            delete.deleteFamily(toBytes(family));
            hTable.delete(delete);

            // set params
            params = new BufferedMutatorParams(tableName);
            params.writeBufferSize(bufferSize);

            // set thread pool
            long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
            ThreadPoolExecutor pool = new ThreadPoolExecutor(10, 256, keepAliveTime, TimeUnit.SECONDS, new SynchronousQueue(), Threads.newDaemonThreadFactory("htable"));
            pool.allowCoreThreadTimeOut(true);
            params.pool(pool);

            ohBufferMutator = connection.getBufferedMutator(params);

            // BufferedMutator is not concurrently safe
            for (int i = 0; i < 50; ++i) {
                final int taskId = i;
                final BufferedMutator thrBufferMutator = ohBufferMutator;
                executorService.submit(() -> {
                    List<Mutation> mutations = new ArrayList<>();
                    for (int j = 0; j < 4; ++j) {
                        String thrColumn = column1 + "_" + taskId + "_" + j;
                        String thrValue = value + "_" + taskId + "_" + j;
                        long thrTimestamp = timestamp;

                        Put put = new Put(Bytes.toBytes(key));
                        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(thrColumn),
                                thrTimestamp, Bytes.toBytes(thrValue));
                        mutations.add(put);
                    }
                    try {
                        thrBufferMutator.mutate(mutations);
                    } catch (Exception ex) {
                        if (ex instanceof RetriesExhaustedWithDetailsException) {
                            ((RetriesExhaustedWithDetailsException) ex).getCauses().get(0).printStackTrace();
                        } else {
                            ex.printStackTrace();
                        }
                        Assert.assertTrue(false);
                    }
                });
            }
        } catch (Exception ex) {
            if (ex instanceof RetriesExhaustedWithDetailsException) {
                ((RetriesExhaustedWithDetailsException) ex).getCauses().get(0).printStackTrace();
            } else {
                ex.printStackTrace();
            }
            Assert.assertTrue(false);
        } finally {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
            if (ohBufferMutator != null) {
                ohBufferMutator.close();
                Get get = new Get(toBytes(key));
                get.addFamily(toBytes(family));
                Result r = hTable.get(get);
                count = r.raw().length;
                Assert.assertEquals(200, count);
                Delete delete = new Delete(toBytes(key));
                delete.deleteFamily(toBytes(family));
                hTable.delete(delete);

                r = hTable.get(get);
                Assert.assertEquals(0, r.raw().length);
                // test add mutations after closed
                final BufferedMutator closedMutator = ohBufferMutator;
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.mutate(delete);
                });
                // test flush after closed
                ohBufferMutator.flush();
            }
            if (params != null) {
                if (params.getPool() != null) {
                    Assert.assertTrue(params.getPool().isShutdown());
                }
            }
        }
    }

    /*
    CREATE TABLEGROUP test_region_locator SHARDING = 'ADAPTIVE';
    CREATE TABLE `test_region_locator$family_region_locator` (
        `K` varbinary(1024) NOT NULL,
        `Q` varbinary(256) NOT NULL,
        `T` bigint(20) NOT NULL,
        `V` varbinary(1024) DEFAULT NULL,
        PRIMARY KEY (`K`, `Q`, `T`)
    ) TABLEGROUP = test_region_locator PARTITION BY RANGE COLUMNS(K) (
        PARTITION p1 VALUES LESS THAN ('c'),
        PARTITION p2 VALUES LESS THAN ('e'),
        PARTITION p3 VALUES LESS THAN ('g'),
        PARTITION p4 VALUES LESS THAN ('i'),
        PARTITION p5 VALUES LESS THAN ('l'),
        PARTITION p6 VALUES LESS THAN ('n'),
        PARTITION p7 VALUES LESS THAN ('p'),
        PARTITION p8 VALUES LESS THAN ('s'),
        PARTITION p9 VALUES LESS THAN ('v'),
        PARTITION p10 VALUES LESS THAN (MAXVALUE)
    );
    */
    @Test
    public void testRangePartitionWithRegionLocator() throws Exception {
        final String tableNameStr = "test_region_locator";
        final String family = "family_region_locator";
        final int regionCount = 10;
        final int rowsPerRegion = 5;

        final byte[][] splitPoints = new byte[][] {
                Bytes.toBytes("c"),  // p1: < 'c'
                Bytes.toBytes("e"),  // p2: < 'e'
                Bytes.toBytes("g"),  // p3: < 'g'
                Bytes.toBytes("i"),  // p4: < 'i'
                Bytes.toBytes("l"),  // p5: < 'l'
                Bytes.toBytes("n"),  // p6: < 'n'
                Bytes.toBytes("p"),  // p7: < 'p'
                Bytes.toBytes("s"),  // p8: < 's'
                Bytes.toBytes("v")   // p9: < 'v'
        };

        final TableName tableName = TableName.valueOf(tableNameStr);
        final Configuration conf = ObHTableTestUtil.newConfiguration();
        connection = ConnectionFactory.createConnection(conf);
        hTable = connection.getTable(tableName);

        try {
            for (int i = 0; i < regionCount; i++) {
                for (int j = 0; j < rowsPerRegion; j++) {
                    String rowKey;
                    if (i == 0) {
                        rowKey = "a" + j;
                    } else if (i == regionCount - 1) {
                        rowKey = "v" + j;
                    } else {
                        String baseKey = Bytes.toString(splitPoints[i - 1]);
                        rowKey = baseKey + j;
                    }

                    Put put = new Put(Bytes.toBytes(rowKey));
                    String value = String.format("value_%d_%d", i, j);
                    put.addColumn(
                            Bytes.toBytes(family),
                            Bytes.toBytes("q"),
                            Bytes.toBytes(value)
                    );
                    hTable.put(put);
                }
            }

            try (RegionLocator locator = connection.getRegionLocator(tableName)) {
                byte[][] startKeys = locator.getStartKeys();
                byte[][] endKeys = locator.getEndKeys();

                Assert.assertEquals("Should have " + regionCount + " regions",
                        regionCount, startKeys.length);

                for (int i = 0; i < startKeys.length; i++) {
                    String startKeyStr = startKeys[i].length == 0 ? "-∞" :
                            Bytes.toString(startKeys[i]);
                    String endKeyStr = endKeys[i].length == 0 ? "+∞" :
                            Bytes.toString(endKeys[i]);
                    // 验证region边界
                    if (i > 0) {
                        // 验证startKey与前一个endKey相同
                        Assert.assertArrayEquals("Region " + i + " startKey should match previous endKey",
                                endKeys[i-1], startKeys[i]);
                    }
                }

                for (int i = 0; i < startKeys.length; i++) {
                    Scan scan = new Scan();
                    if (startKeys[i].length > 0) {
                        scan.setStartRow(startKeys[i]);
                    }
                    if (endKeys[i].length > 0) {
                        scan.setStopRow(endKeys[i]);
                    }

                    String startKeyStr = startKeys[i].length == 0 ? "-∞" :
                            Bytes.toString(startKeys[i]);
                    String endKeyStr = endKeys[i].length == 0 ? "+∞" :
                            Bytes.toString(endKeys[i]);

                    try (ResultScanner scanner = hTable.getScanner(scan)) {
                        List<Result> results = new ArrayList<>();
                        for (Result result : scanner) {
                            results.add(result);
                        }

                        Assert.assertEquals("Region " + i + " should have " + rowsPerRegion + " rows",
                                rowsPerRegion, results.size());

                        for (Result result : results) {
                            String rowKey = Bytes.toString(result.getRow());
                            String value = Bytes.toString(result.getValue(
                                    Bytes.toBytes(family), Bytes.toBytes("q")));
                            if (startKeys[i].length > 0) {
                                Assert.assertTrue("Row key " + rowKey + " should be >= " +
                                                Bytes.toString(startKeys[i]),
                                        rowKey.compareTo(Bytes.toString(startKeys[i])) >= 0);
                            }
                            if (endKeys[i].length > 0) {
                                Assert.assertTrue("Row key " + rowKey + " should be < " +
                                                Bytes.toString(endKeys[i]),
                                        rowKey.compareTo(Bytes.toString(endKeys[i])) < 0);
                            }
                        }
                    }
                }
            }
        } finally {
            Optional.ofNullable(hTable).ifPresent(table -> {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            Optional.ofNullable(connection).ifPresent(conn -> {
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }


    @Test
    public void testCreateTable() throws Exception {
        final Configuration conf = ObHTableTestUtil.newConfiguration();
        connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        HTableDescriptor descriptor = new HTableDescriptor();
        descriptor.setName("test".getBytes());
        HColumnDescriptor cf1Desc = new HColumnDescriptor("family1".getBytes());
        cf1Desc.setTimeToLive(100);
        cf1Desc.setMaxVersions(20);
        HColumnDescriptor cf2Desc = new HColumnDescriptor("family2".getBytes());
        HColumnDescriptor cf3Desc = new HColumnDescriptor("family3".getBytes());
        cf3Desc.setTimeToLive(30);
        descriptor.addFamily(cf1Desc);
        descriptor.addFamily(cf2Desc);
        descriptor.addFamily(cf3Desc);
        admin.createTable(descriptor);
    }
}
