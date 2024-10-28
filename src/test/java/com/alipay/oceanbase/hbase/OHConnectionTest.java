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

import com.alipay.oceanbase.hbase.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static com.alipay.oceanbase.hbase.constants.OHConstants.SOCKET_TIMEOUT;
import static org.apache.hadoop.hbase.ipc.RpcClient.SOCKET_TIMEOUT_CONNECT;
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

    @BeforeClass
    public static void before() throws Exception {
        // use self-defined namespace "n1"
        hTable = ObHTableTestUtil.newOHTableClient("n1:test");
        ((OHTableClient) hTable).init();
    }

    @AfterClass
    public static void finish() throws IOException {
        hTable.close();
    }

    @Test
    public void testRefreshTableEntry() throws Exception {
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
        hTable.close();
    }

    private void testBasic() throws Exception {
        String key = "putKey";
        String column1 = "putColumn1";
        String column2 = "putColumn2";
        String value = "value";
        String family = "family1";
        long timestamp = System.currentTimeMillis();
        Delete delete = new Delete(key.getBytes());
        delete.addFamily(family.getBytes());
        hTable.delete(delete);

        Put put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column1.getBytes(), timestamp, toBytes(value));
        hTable.put(put);
        Get get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), toBytes(column1));
        Result r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        for (Cell keyValue : r.rawCells()) {
            Assert.assertEquals(key, Bytes.toString(CellUtil.cloneRow(keyValue)));
            Assert.assertEquals(column1, Bytes.toString(CellUtil.cloneQualifier(keyValue)));
            Assert.assertEquals(timestamp, keyValue.getTimestamp());
            Assert.assertEquals(value, Bytes.toString(CellUtil.cloneValue(keyValue)));
        }

        put = new Put(toBytes(key));
        put.addColumn(family.getBytes(), column1.getBytes(), timestamp + 1, toBytes(value));
        hTable.put(put);
        get = new Get(toBytes(key));
        get.addColumn(family.getBytes(), toBytes(column1));
        get.setMaxVersions(2);
        r = hTable.get(get);
        Assert.assertEquals(2, r.rawCells().length);

        get.setMaxVersions(1);
        r = hTable.get(get);
        Assert.assertEquals(1, r.rawCells().length);

        delete = new Delete(key.getBytes());
        delete.addFamily(family.getBytes());
        hTable.delete(delete);

        for (Cell keyValue : r.rawCells()) {
            System.out.println("rowKey: " + new String(CellUtil.cloneRow(keyValue)) + " columnQualifier:"
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
                    Assert.assertEquals(key + "_" + i, Bytes.toString(CellUtil.cloneRow(keyValue)));
                    Assert.assertTrue(column1.equals(Bytes.toString(CellUtil.cloneQualifier(keyValue)))
                                      || column2.equals(Bytes.toString(CellUtil.cloneQualifier(keyValue))));
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
                    Assert.assertEquals(key + "_" + i, Bytes.toString(CellUtil.cloneRow(keyValue)));
                    Assert.assertTrue(column1.equals(Bytes.toString(CellUtil.cloneQualifier(keyValue)))
                                      || column2.equals(Bytes.toString(CellUtil.cloneQualifier(keyValue))));
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
        BufferedMutator putBufferMutator = null;
        BufferedMutator delBufferedMutator = null;
        String key = "putKey";
        String column1 = "putColumn1";
        String value = "value333444";
        String family = "family_group";
        try {
            TableName tableName = TableName.valueOf("test");
            connection = ConnectionFactory.createConnection(conf);
            hTable = connection.getTable(tableName);

            Delete delete= new Delete(toBytes(key));
            delete.addFamily(toBytes(family));
            hTable.delete(delete);

            // use defualt params
            putBufferMutator = connection.getBufferedMutator(tableName);
            delBufferedMutator = connection.getBufferedMutator(tableName);


            long timestamp = System.currentTimeMillis();

            // only support Put and Delete
            // for other type of operations, BufferedMutator will not set its type for them
            Append append = new Append(Bytes.toBytes(key));
            append.add("family_group".getBytes(), column1.getBytes(), toBytes("_suffix"));
            final BufferedMutator apMut = putBufferMutator;
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
            putBufferMutator.mutate(mutations);
            putBufferMutator.flush();

            Get get = new Get(toBytes(key));
            Result r = hTable.get(get);
            Assert.assertEquals(2, r.rawCells().length);

            Delete del = new Delete(Bytes.toBytes(key));
            final BufferedMutator noCfMut = putBufferMutator;
            // test mutation without setting family
            Assert.assertThrows(FeatureNotSupportedException.class, () -> {
                noCfMut.mutate(del);
            });
            del.addFamily(Bytes.toBytes(family));
            // test reuse different type bufferedMutator
            final BufferedMutator difTypeMut = putBufferMutator;
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                difTypeMut.mutate(del);
            });
            // test add Mutation directly
            delBufferedMutator.mutate(del);
            delBufferedMutator.flush();

            r = hTable.get(get);
            Assert.assertEquals(0, r.rawCells().length);
        } catch (Exception ex) {
            if (ex instanceof RetriesExhaustedWithDetailsException) {
                ((RetriesExhaustedWithDetailsException) ex).getCauses().get(0).printStackTrace();
            } else {
                ex.printStackTrace();
            }
            Assert.assertTrue(false);
        } finally {
            if (putBufferMutator != null ) {
                putBufferMutator.close();
                // test flush after closed
                putBufferMutator.flush();
            }
            if (delBufferedMutator != null) {
                delBufferedMutator.close();
                // test add mutations after closed
                Delete delete = new Delete(Bytes.toBytes(key));
                delete.addFamily(Bytes.toBytes(family));
                final BufferedMutator closedMutator = delBufferedMutator;
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
        BufferedMutator putBufferMutator = null;
        BufferedMutator delBufferedMutator = null;
        String key = "putKey";
        String column1 = "putColumn1";
        String value = "value333444";
        String family = "family_group";
        long timestamp = System.currentTimeMillis();
        try {
            // use n1 database
            TableName tableName = TableName.valueOf("n1","test");
            connection = ConnectionFactory.createConnection(conf);
            hTable = connection.getTable(tableName);

            Delete delete= new Delete(toBytes(key));
            delete.addFamily(toBytes(family));
            hTable.delete(delete);

            // use defualt params
            putBufferMutator = connection.getBufferedMutator(tableName);
            delBufferedMutator = connection.getBufferedMutator(tableName);

            // only support Put and Delete
            // for other type of operations, BufferedMutator will not set its type for them
            Append append = new Append(Bytes.toBytes(key));
            append.add(toBytes(family), toBytes(column1), toBytes("_suffix"));
            final BufferedMutator apMut = putBufferMutator;
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
            putBufferMutator.mutate(mutations);
            putBufferMutator.flush();

            Get get = new Get(toBytes(key));
            Result r = hTable.get(get);
            Assert.assertEquals(2, r.rawCells().length);

            Delete del = new Delete(Bytes.toBytes(key));
            final BufferedMutator noCfMut = putBufferMutator;
            // test mutation without setting family
            Assert.assertThrows(FeatureNotSupportedException.class, () -> {
                noCfMut.mutate(del);
            });
            del.addFamily(Bytes.toBytes(family));
            final BufferedMutator difTypeMut = putBufferMutator;
            // test reuse different type bufferedMutator
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                difTypeMut.mutate(del);
            });
            // test add Mutation directly
            delBufferedMutator.mutate(del);
            delBufferedMutator.flush();

            r = hTable.get(get);
            Assert.assertEquals(0, r.rawCells().length);
        } catch (Exception ex) {
            if (ex instanceof RetriesExhaustedWithDetailsException) {
                ((RetriesExhaustedWithDetailsException) ex).getCauses().get(0).printStackTrace();
            } else {
                ex.printStackTrace();
            }
            Assert.assertTrue(false);
        } finally {
            if (putBufferMutator != null ) {
                putBufferMutator.close();
                // test flush after closed
                putBufferMutator.flush();
            }
            if (delBufferedMutator != null) {
                delBufferedMutator.close();
                // test add mutations after closed
                Delete delete = new Delete(Bytes.toBytes("putKey"));
                delete.addFamily(Bytes.toBytes("family_group"));
                final BufferedMutator closedMutator = delBufferedMutator;
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
        BufferedMutator putBufferMutator = null;
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
            delete.addFamily(toBytes(family));
            hTable.delete(delete);

            // set params
            params = new BufferedMutatorParams(tableName);
            params.writeBufferSize(bufferSize);
            putBufferMutator = connection.getBufferedMutator(params);

            List<Mutation> mutations = new ArrayList<>();
            for (int i = 0; i < 50; ++i) {
                mutations.clear();
                for (int j = 0; j < 4; ++j) {
                    Put put = new Put(Bytes.toBytes(key));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column1 + "_" + i + "_" + j),
                            timestamp, Bytes.toBytes(value + "_" + i + "_" + j));
                    mutations.add(put);
                }
                putBufferMutator.mutate(mutations);
            }
        } catch (Exception ex) {
            if (ex instanceof RetriesExhaustedWithDetailsException) {
                ((RetriesExhaustedWithDetailsException) ex).getCauses().get(0).printStackTrace();
            } else {
                ex.printStackTrace();
            }
            Assert.assertTrue(false);
        } finally {
            if (putBufferMutator != null) {
                putBufferMutator.close();
                Get get = new Get(toBytes(key));
                Result r = hTable.get(get);
                for (Cell keyValue : r.rawCells()) {
                    ++count;
                }
                Assert.assertEquals(200, count);
                Delete delete = new Delete(toBytes(key));
                delete.addFamily(toBytes(family));
                hTable.delete(delete);
                r = hTable.get(get);
                Assert.assertEquals(0, r.rawCells().length);

                // test add mutations after closed
                final BufferedMutator closedMutator = putBufferMutator;
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.mutate(delete);
                });
                // test flush after closed
                putBufferMutator.flush();
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
            delete.addFamily(toBytes(family));
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
                ohBufferMutator.mutate(mutations);
            }
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
                Result r = hTable.get(get);
                for (Cell keyValue : r.rawCells()) {
                    ++count;
                }
                Assert.assertEquals(200, count);
                Delete delete = new Delete(toBytes(key));
                delete.addFamily(toBytes(family));
                hTable.delete(delete);

                r = hTable.get(get);
                Assert.assertEquals(0, r.rawCells().length);
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
            delete.addFamily(toBytes(family));
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

            for (int i = 0; i < 50; ++i) {
                final int taskId = i;
                final BufferedMutator thrBufferMutator = ohBufferMutator;
                executorService.submit(() -> {
                    List<Mutation> mutations = new ArrayList<>();
                    for (int j = 0; j < 4; ++j) {
                        String thrKey = key;
                        String thrColumn = column1 + "_" + taskId + "_" + j;
                        String thrValue = value + "_" + taskId + "_" + j;
                        long thrTimestamp = timestamp;

                        Put put = new Put(Bytes.toBytes(thrKey));
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
                Result r = hTable.get(get);
                for (Cell keyValue : r.rawCells()) {
                    ++count;
                }
                Assert.assertEquals(200, count);
                Delete delete = new Delete(toBytes(key));
                delete.addFamily(toBytes(family));
                hTable.delete(delete);

                r = hTable.get(get);
                Assert.assertEquals(0, r.rawCells().length);
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
}
