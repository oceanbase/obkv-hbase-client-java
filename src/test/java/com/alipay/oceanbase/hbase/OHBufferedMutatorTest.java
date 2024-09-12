package com.alipay.oceanbase.hbase;

import com.alipay.oceanbase.hbase.util.OHBufferedMutatorImpl;
import net.bytebuddy.implementation.bytecode.Throw;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHBufferedMutatorTest {
    protected Table hTable;
    protected  Configuration conf;
    @Before
    public void setup() throws IOException {
        conf = ObHTableTestUtil.newConfiguration();
        conf.set("rs.list.acquire.read.timeout", "10000");
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
        hTable = new OHTable(conf, "test");
        BufferedMutator ohBufferMutator = null;
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            TableName tableName = TableName.valueOf("test");

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

            // test NOT_SUPPORT type'
            Put put = new Put(Bytes.toBytes(key));
            put.addColumn(Bytes.toBytes("family_group"), Bytes.toBytes(column1), timestamp, Bytes.toBytes(value));
            put.addColumn(Bytes.toBytes("family_group1"), Bytes.toBytes(column1 + "1"), timestamp, Bytes.toBytes(value + "4"));
            // not support different family in one batch
            final BufferedMutator difMut = ohBufferMutator;
            final Put difPut= put;
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                difMut.mutate(difPut);
            });

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
        hTable = new OHTable(conf, "n1:test");
        BufferedMutator ohBufferMutator = null;
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            // in "n1" database
            TableName tableName = TableName.valueOf("n1:test");

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

            // test NOT_SUPPORT type
            Put put = new Put(Bytes.toBytes(key));
            put.addColumn(Bytes.toBytes("family_group"), Bytes.toBytes(column1), timestamp, Bytes.toBytes(value));
            put.addColumn(Bytes.toBytes("family_group1"), Bytes.toBytes(column1 + "1"), timestamp, Bytes.toBytes(value + "4"));
            // not support different family in one batch
            final BufferedMutator difMut = ohBufferMutator;
            final Put difPut = put;
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                difMut.mutate(difPut);
            });

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
        hTable = new OHTable(conf, "test");
        BufferedMutator ohBufferMutator = null;
        BufferedMutatorParams params = null;
        long bufferSize = 45000L;
        int count = 0;
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            // use default database
            TableName tableName = TableName.valueOf("test");

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
        hTable = new OHTable(conf, "test");
        BufferedMutator ohBufferMutator = null;
        BufferedMutatorParams params = null;
        long bufferSize = 45000L;
        int count = 0;
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            // use default database
            TableName tableName = TableName.valueOf("test");

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
        hTable = new OHTable(conf, "test");
        BufferedMutator ohBufferMutator = null;
        BufferedMutatorParams params = null;
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        long bufferSize = 45000L;
        int count = 0;
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            // use default database
            TableName tableName = TableName.valueOf("test");

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
