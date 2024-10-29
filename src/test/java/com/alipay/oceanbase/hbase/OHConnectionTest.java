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

import com.alipay.oceanbase.hbase.util.OHBufferedMutatorImpl;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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

    @After
    public void after() throws IOException {
        hTable.close();
    }

    @Test
    public void testGetTableByTableBuilder() throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        conf.set("rs.list.acquire.read.timeout", "10000");
        conf.set(SOCKET_TIMEOUT_CONNECT, "15000");
        connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("test");
        TableBuilder builder = connection.getTableBuilder(tableName, null);
        // build a OHTable with default params
        hTable = builder.build();
        testBasic();

        // set params for TableBuilder
        long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
        ThreadPoolExecutor pool = new ThreadPoolExecutor(10, 256, keepAliveTime, TimeUnit.SECONDS,
            new SynchronousQueue(), Threads.newDaemonThreadFactory("htable"));
        pool.allowCoreThreadTimeOut(true);
        builder = connection.getTableBuilder(tableName, pool);
        builder.setRpcTimeout(40000);
        builder.setReadRpcTimeout(30000);
        builder.setWriteRpcTimeout(50000);
        builder.setOperationTimeout(1500000);
        hTable = builder.build();
        Assert.assertEquals(40000, hTable.getRpcTimeout());
        Assert.assertEquals(30000, hTable.getReadRpcTimeout());
        Assert.assertEquals(50000, hTable.getWriteRpcTimeout());
        Assert.assertEquals(1500000, hTable.getOperationTimeout());
        testBasic();

        hTable.close();
        Assert.assertFalse(pool.isShutdown());
        pool.shutdown();
        try {
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                System.out
                    .println("close() failed to terminate pool after 10 seconds. Abandoning pool.");
            }
        } catch (InterruptedException e) {
            System.out.println("waitForTermination interrupted");
            Thread.currentThread().interrupt();
        }
        Assert.assertTrue(pool.isShutdown());
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
            delete.addFamily(toBytes(family));
            hTable.delete(delete);

            // use defualt params
            bufferMutator = connection.getBufferedMutator(tableName);


            long timestamp = System.currentTimeMillis();

            // only support Put and Delete
            Append append = new Append(Bytes.toBytes(key));
            append.addColumn("family_group".getBytes(), column1.getBytes(), toBytes("_suffix"));
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
            Assert.assertEquals(2, r.rawCells().length);

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
            Assert.assertEquals(3, r.rawCells().length);

            // test Delete
            Delete del = new Delete(toBytes(key));
            del.addFamily(toBytes(family));
            // test without setting family, delete all
            bufferMutator.mutate(del);
            bufferMutator.flush();

            r = hTable.get(get);
            Assert.assertEquals(0, r.rawCells().length);

            // test hybrid mutations
            mutations.clear();
            mutations.add(put1);
            mutations.add(put2);
            mutations.add(del);
            mutations.add(put3);
            bufferMutator.mutate(mutations);
            bufferMutator.flush();

            r = hTable.get(get);
            Assert.assertEquals(1, r.rawCells().length);
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
                final BufferedMutator closedMutator = bufferMutator;
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.flush();
                });
                // test add mutations after closed
                Delete delete = new Delete(Bytes.toBytes(key));
                delete.addFamily(Bytes.toBytes(family));
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.mutate(delete);
                });
                if (((OHBufferedMutatorImpl) bufferMutator).getPool() != null) {
                    Assert.assertTrue(((OHBufferedMutatorImpl) bufferMutator).getPool().isShutdown());
                }
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
            Assert.assertEquals("n1:test", hTable.getName().getNameAsString());

            Delete delete= new Delete(toBytes(key));
            delete.addFamily(toBytes(family));
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
            Assert.assertEquals(2, r.rawCells().length);

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
            Assert.assertEquals(3, r.rawCells().length);

            // test Delete
            Delete del = new Delete(toBytes(key));
            // test without setting family, delete all
            bufferMutator.mutate(del);
            bufferMutator.flush();

            r = hTable.get(get);
            Assert.assertEquals(0, r.rawCells().length);

            // test hybrid mutations
            mutations.clear();
            mutations.add(put1);
            mutations.add(put2);
            mutations.add(del);
            mutations.add(put3);
            bufferMutator.mutate(mutations);
            bufferMutator.flush();

            r = hTable.get(get);
            Assert.assertEquals(1, r.rawCells().length);
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
                final BufferedMutator closedMutator = bufferMutator;
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.flush();
                });
                // test add mutations after closed
                Delete delete = new Delete(Bytes.toBytes(key));
                delete.addFamily(Bytes.toBytes(family));
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.mutate(delete);
                });
                if (((OHBufferedMutatorImpl) bufferMutator).getPool() != null) {
                    Assert.assertTrue(((OHBufferedMutatorImpl) bufferMutator).getPool().isShutdown());
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
            delete.addFamily(toBytes(family));
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
            count = r.rawCells().length;
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
                count = r.rawCells().length;
                Assert.assertEquals(180, count);
                Delete delete = new Delete(toBytes(key));
                delete.addFamily(toBytes(family));
                hTable.delete(delete);
                r = hTable.get(get);
                Assert.assertEquals(0, r.rawCells().length);

                // test add mutations after closed
                final BufferedMutator closedMutator = bufferMutator;
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.mutate(delete);
                });
                // test flush after closed
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.flush();
                });
                if (((OHBufferedMutatorImpl) bufferMutator).getPool() != null) {
                    Assert.assertTrue(((OHBufferedMutatorImpl) bufferMutator).getPool().isShutdown());
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
        ThreadPoolExecutor pool = null;
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
            pool = new ThreadPoolExecutor(10, 256, keepAliveTime, TimeUnit.SECONDS, new SynchronousQueue(), Threads.newDaemonThreadFactory("htable"));
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
            count = r.rawCells().length;
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
                count = r.rawCells().length;
                Assert.assertEquals(180, count);
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
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.flush();
                });
                ExecutorService bufferPool = ((OHBufferedMutatorImpl) ohBufferMutator).getPool();
                if (bufferPool != null) {
                    // self-defined pool must be shutdown by users
                    Assert.assertFalse(bufferPool.isShutdown());
                    bufferPool.shutdown();
                    try {
                        if (!bufferPool.awaitTermination(10, TimeUnit.SECONDS)) {
                            System.out.println("close() failed to terminate pool after 10 seconds. Abandoning pool.");
                        }
                    } catch (InterruptedException e) {
                        System.out.println("waitForTermination interrupted");
                        Thread.currentThread().interrupt();
                    }
                    Assert.assertTrue(bufferPool.isShutdown());
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
        ThreadPoolExecutor pool = null;
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
            pool = new ThreadPoolExecutor(10, 256, keepAliveTime, TimeUnit.SECONDS, new SynchronousQueue(), Threads.newDaemonThreadFactory("htable"));
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
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
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
                count = r.rawCells().length;
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
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.flush();
                });
                ExecutorService bufferPool = ((OHBufferedMutatorImpl) ohBufferMutator).getPool();
                if (bufferPool != null) {
                    // self-defined pool must be shutdown by users
                    Assert.assertFalse(bufferPool.isShutdown());
                    bufferPool.shutdown();
                    try {
                        if (!bufferPool.awaitTermination(10, TimeUnit.SECONDS)) {
                            System.out.println("close() failed to terminate pool after 10 seconds. Abandoning pool.");
                        }
                    } catch (InterruptedException e) {
                        System.out.println("waitForTermination interrupted");
                        Thread.currentThread().interrupt();
                    }
                    Assert.assertTrue(bufferPool.isShutdown());
                }
            }
        }
    }

    @Test
    public void testBufferedMutatorPeriodicFlush() throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        conf.set("rs.list.acquire.read.timeout", "10000");
        BufferedMutator ohBufferMutator = null;
        BufferedMutatorParams params = null;
        ThreadPoolExecutor pool = null;
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
            // set periodic flush timeout to enable Timer
            params.setWriteBufferPeriodicFlushTimeoutMs(100);

            // set thread pool
            long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
            pool = new ThreadPoolExecutor(10, 256, keepAliveTime, TimeUnit.SECONDS, new SynchronousQueue(), Threads.newDaemonThreadFactory("htable"));
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

            // test auto flush
            Get get = new Get(toBytes(key));
            get.setMaxVersions();
            get.addFamily(toBytes(family));
            Result r = hTable.get(get);
            count = r.rawCells().length;
            Assert.assertTrue(count > 0);

            // test timer periodic flush
            int lastUndealtCount = ((OHBufferedMutatorImpl) ohBufferMutator).size();
            Thread.sleep(1000);
            int currentUndealtCount = ((OHBufferedMutatorImpl) ohBufferMutator).size();
            Assert.assertNotEquals(lastUndealtCount, currentUndealtCount);
            // after periodic flush, all mutations will be committed
            Assert.assertEquals(0, currentUndealtCount);
            r = hTable.get(get);
            int newCount = r.rawCells().length;
            Assert.assertNotEquals(count, newCount);
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
                count = r.rawCells().length;
                Assert.assertEquals(180, count);
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
                Assert.assertThrows(IllegalStateException.class, () -> {
                    closedMutator.flush();
                });
                ExecutorService bufferPool = ((OHBufferedMutatorImpl) ohBufferMutator).getPool();
                if (bufferPool != null) {
                    // self-defined pool must be shutdown by users
                    Assert.assertFalse(bufferPool.isShutdown());
                    bufferPool.shutdown();
                    try {
                        if (!bufferPool.awaitTermination(10, TimeUnit.SECONDS)) {
                            System.out.println("close() failed to terminate pool after 10 seconds. Abandoning pool.");
                        }
                    } catch (InterruptedException e) {
                        System.out.println("waitForTermination interrupted");
                        Thread.currentThread().interrupt();
                    }
                    Assert.assertTrue(bufferPool.isShutdown());
                }
            }
        }
    }
}
