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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHConnectionTest {
    protected Table      hTable;
    protected Connection connection;

    @Test
    public void testConnectionBySet() throws Exception {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL,
            "com.alipay.oceanbase.hbase.util.OHConnectionImpl");
        c.set("rs.list.acquire.read.timeout", "10000");
        connection = ConnectionFactory.createConnection(c);
        TableName tableName = TableName.valueOf("test");
        hTable = connection.getTable(tableName);
        testBasic();
    }

    @Test
    public void testConnectionByXml() throws Exception {

        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        connection = ConnectionFactory.createConnection(c);
        TableName tableName = TableName.valueOf("test");
        hTable = connection.getTable(tableName);
        testBasic();
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
        TableName tableName = TableName.valueOf("test");
        connection = ConnectionFactory.createConnection(conf);
        hTable = connection.getTable(tableName);
        BufferedMutator ohBufferMutator = null;
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            // use defualt params
            ohBufferMutator = conn.getBufferedMutator(tableName);

            String key = "putKey";
            String column1 = "putColumn1";
            String value = "value333444";
            long timestamp = System.currentTimeMillis();

            List<Mutation> mutations = new ArrayList<>();
            // test Put
            Put put1 = new Put(Bytes.toBytes(key));
            put1.addColumn(Bytes.toBytes("family_group"), Bytes.toBytes(column1), timestamp, Bytes.toBytes(value));
            mutations.add(put1);
            Put put2 = new Put(Bytes.toBytes(key));
            put2.addColumn(Bytes.toBytes("family_group"), Bytes.toBytes(column1 + "1"), timestamp, Bytes.toBytes(value + "4"));
            mutations.add(put2);
            // test add Mutations with List
            ohBufferMutator.mutate(mutations);
            ohBufferMutator.flush();

            Get get = new Get(toBytes(key));
            Result r = hTable.get(get);
            Assert.assertEquals(2, r.raw().length);
            for (KeyValue keyValue : r.raw()) {
                System.out.println("rowKey: " + new String(keyValue.getRow()) + " family :"
                        + new String(keyValue.getFamily()) + " columnQualifier:"
                        + new String(keyValue.getQualifier()) + " timestamp:"
                        + keyValue.getTimestamp() + " value:"
                        + new String(keyValue.getValue()));
            }

            Delete del = new Delete(Bytes.toBytes(key));
            del.deleteFamily(Bytes.toBytes("family_group"));
            // test add Mutation directly
            ohBufferMutator.mutate(del);
            ohBufferMutator.flush();

            r = hTable.get(get);
            Assert.assertEquals(0, r.raw().length);

            Append append = new Append(Bytes.toBytes(key));
            append.add("family_group".getBytes(), column1.getBytes(), toBytes("_suffix"));
            // only support Put and Delete
            final BufferedMutator apMut = ohBufferMutator;
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                apMut.mutate(append);
            });
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
            }
        }
    }

    /*
    CREATE TABLEGROUP n1:test SHARDING = 'ADAPTIVE';
    CREATE TABLE `n1:test$family_group` (
                  `K` varbinary(1024) NOT NULL,
                  `Q` varbinary(256) NOT NULL,
                  `T` bigint(20) NOT NULL,
                  `V` varbinary(1024) DEFAULT NULL,
                  PRIMARY KEY (`K`, `Q`, `T`)
            ) TABLEGROUP = n1:test;
     */
    @Test
    public void testBufferedMutatorUseNameSpaceWithFlush() throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        conf.set("rs.list.acquire.read.timeout", "10000");
        // in "n1" database
        TableName tableName = TableName.valueOf("n1:test");
        connection = ConnectionFactory.createConnection(conf);
        hTable = connection.getTable(tableName);
        BufferedMutator ohBufferMutator = null;
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            // use defualt params
            ohBufferMutator = conn.getBufferedMutator(tableName);

            String key = "putKey";
            String column1 = "putColumn1";
            String value = "value333444";
            long timestamp = System.currentTimeMillis();

            List<Mutation> mutations = new ArrayList<>();
            // test Put
            Put put1 = new Put(Bytes.toBytes(key));
            put1.addColumn(Bytes.toBytes("family_group"), Bytes.toBytes(column1), timestamp, Bytes.toBytes(value));
            mutations.add(put1);
            Put put2 = new Put(Bytes.toBytes(key));
            put2.addColumn(Bytes.toBytes("family_group"), Bytes.toBytes(column1 + "1"), timestamp, Bytes.toBytes(value + "4"));
            mutations.add(put2);
            // test add Mutations with List
            ohBufferMutator.mutate(mutations);
            ohBufferMutator.flush();

            Get get = new Get(toBytes(key));
            Result r = hTable.get(get);
            Assert.assertEquals(2, r.raw().length);
            for (KeyValue keyValue : r.raw()) {
                System.out.println("rowKey: " + new String(keyValue.getRow()) + " family :"
                        + new String(keyValue.getFamily()) + " columnQualifier:"
                        + new String(keyValue.getQualifier()) + " timestamp:"
                        + keyValue.getTimestamp() + " value:"
                        + new String(keyValue.getValue()));
            }

            // test Delete
            Delete del = new Delete(Bytes.toBytes(key));
            del.deleteFamily(Bytes.toBytes("family_group"));
            // test add Mutation directly
            ohBufferMutator.mutate(del);
            ohBufferMutator.flush();

            r = hTable.get(get);
            Assert.assertEquals(0, r.raw().length);

            Append append = new Append(Bytes.toBytes(key));
            append.add("family_group".getBytes(), column1.getBytes(), toBytes("_suffix"));
            // only support Put and Delete
            final BufferedMutator apMut = ohBufferMutator;
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                apMut.mutate(append);
            });
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
        TableName tableName = TableName.valueOf("test");
        connection = ConnectionFactory.createConnection(conf);
        hTable = connection.getTable(tableName);
        BufferedMutator ohBufferMutator = null;
        BufferedMutatorParams params = null;
        long bufferSize = 45000L;
        int count = 0;
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            // set params
            params = new BufferedMutatorParams(tableName);
            params.writeBufferSize(bufferSize);

            ohBufferMutator = conn.getBufferedMutator(params);

            String key = "putKey";
            String column1 = "putColumn1";
            String value = "value333444";
            long timestamp = System.currentTimeMillis();

            List<Mutation> mutations = new ArrayList<>();
            for (int i = 0; i < 50; ++i) {
                mutations.clear();
                for (int j = 0; j < 4; ++j) {
                    Put put = new Put(Bytes.toBytes(key));
                    put.addColumn(Bytes.toBytes("family_group"), Bytes.toBytes(column1 + "_" + i + "_" + j),
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
                Get get = new Get(toBytes("putKey"));
                Result r = hTable.get(get);
                for (KeyValue keyValue : r.raw()) {
                    ++count;
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " family :"
                            + new String(keyValue.getFamily()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                }
                Assert.assertEquals(200, count);
                Delete delete = new Delete(toBytes("putKey"));
                delete.deleteFamily(toBytes("family_group"));
                hTable.delete(delete);

                r = hTable.get(get);
                Assert.assertEquals(0, r.raw().length);
            }
            if (params != null) {
                if (params.getPool() != null) {
                    System.out.println("Check if user's pool is shut down.");
                    Assert.assertTrue(params.getPool().isShutdown());
                }
            }
        }
    }

    @Test
    public void testBufferedMutatorWithUserPool() throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        conf.set("rs.list.acquire.read.timeout", "10000");
        TableName tableName = TableName.valueOf("test");
        connection = ConnectionFactory.createConnection(conf);
        hTable = connection.getTable(tableName);
        BufferedMutator ohBufferMutator = null;
        BufferedMutatorParams params = null;
        long bufferSize = 45000L;
        int count = 0;
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            // set params
            params = new BufferedMutatorParams(tableName);
            params.writeBufferSize(bufferSize);

            // set thread pool
            long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
            ThreadPoolExecutor pool = new ThreadPoolExecutor(10, 256, keepAliveTime, TimeUnit.SECONDS, new SynchronousQueue(), Threads.newDaemonThreadFactory("htable"));
            pool.allowCoreThreadTimeOut(true);
            params.pool(pool);

            ohBufferMutator = conn.getBufferedMutator(params);

            String key = "putKey";
            String column1 = "putColumn1";
            String value = "value333444";
            long timestamp = System.currentTimeMillis();

            List<Mutation> mutations = new ArrayList<>();
            for (int i = 0; i < 50; ++i) {
                mutations.clear();
                for (int j = 0; j < 4; ++j) {
                    Put put = new Put(Bytes.toBytes(key));
                    put.addColumn(Bytes.toBytes("family_group"), Bytes.toBytes(column1 + "_" + i + "_" + j),
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
                Get get = new Get(toBytes("putKey"));
                Result r = hTable.get(get);
                for (KeyValue keyValue : r.raw()) {
                    ++count;
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " family :"
                            + new String(keyValue.getFamily()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                }
                Assert.assertEquals(200, count);
                Delete delete = new Delete(toBytes("putKey"));
                delete.deleteFamily(toBytes("family_group"));
                hTable.delete(delete);

                r = hTable.get(get);
                Assert.assertEquals(0, r.raw().length);
            }
            if (params != null) {
                if (params.getPool() != null) {
                    System.out.println("Check if user's pool is shut down.");
                    Assert.assertTrue(params.getPool().isShutdown());
                }
            }
        }
    }

    @Test
    public void testBufferedMutatorConcurrent() throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        conf.set("rs.list.acquire.read.timeout", "10000");
        TableName tableName = TableName.valueOf("test");
        connection = ConnectionFactory.createConnection(conf);
        hTable = connection.getTable(tableName);
        BufferedMutator ohBufferMutator = null;
        BufferedMutatorParams params = null;
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        long bufferSize = 45000L;
        int count = 0;
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            // set params
            params = new BufferedMutatorParams(tableName);
            params.writeBufferSize(bufferSize);

            // set thread pool
            long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
            ThreadPoolExecutor pool = new ThreadPoolExecutor(10, 256, keepAliveTime, TimeUnit.SECONDS, new SynchronousQueue(), Threads.newDaemonThreadFactory("htable"));
            pool.allowCoreThreadTimeOut(true);
            params.pool(pool);

            ohBufferMutator = conn.getBufferedMutator(params);

            String key = "putKey";
            String column1 = "putColumn1";
            String value = "value333444";
            long timestamp = System.currentTimeMillis();

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
                        put.addColumn(Bytes.toBytes("family_group"), Bytes.toBytes(thrColumn),
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
                Get get = new Get(toBytes("putKey"));
                Result r = hTable.get(get);
                for (KeyValue keyValue : r.raw()) {
                    ++count;
                    System.out.println("rowKey: " + new String(keyValue.getRow()) + " family :"
                            + new String(keyValue.getFamily()) + " columnQualifier:"
                            + new String(keyValue.getQualifier()) + " timestamp:"
                            + keyValue.getTimestamp() + " value:"
                            + new String(keyValue.getValue()));
                }
                Assert.assertEquals(200, count);
                Delete delete = new Delete(toBytes("putKey"));
                delete.deleteFamily(toBytes("family_group"));
                hTable.delete(delete);

                r = hTable.get(get);
                Assert.assertEquals(0, r.raw().length);
            }
            if (params != null) {
                if (params.getPool() != null) {
                    System.out.println("Check if user's pool is shut down.");
                    Assert.assertTrue(params.getPool().isShutdown());
                }
            }
        }
    }
}
