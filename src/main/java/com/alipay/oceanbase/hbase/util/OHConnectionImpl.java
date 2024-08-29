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

package com.alipay.oceanbase.hbase.util;

import com.alipay.oceanbase.hbase.OHTable;
import com.alipay.oceanbase.hbase.exception.FeatureNotSupportedException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Threads;
import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.IOException;
import java.util.concurrent.*;

@InterfaceAudience.Private
public class OHConnectionImpl implements Connection {

    private static final Logger             LOGGER               = TableHBaseLoggerFactory
                                                                     .getLogger(OHConnectionImpl.class);

    private static final Marker             FATAL                = MarkerFactory.getMarker("FATAL");

    private static final int                BUFFERED_PARAM_UNSET = -1;

    private volatile boolean                closed;
    private volatile boolean                aborted;

    // thread executor shared by all HTableInterface instances created
    // by this connection
    private volatile ExecutorService        batchPool            = null;

    // If the pool is internally generated, it needs to be released when closing.
    private volatile boolean                cleanupPool          = false;

    private final Configuration             conf;

    private final OHConnectionConfiguration connectionConfig;

    OHConnectionImpl(Configuration conf, final boolean managed, ExecutorService pool,
                     final User user) throws IOException {
        this.conf = conf;
        this.batchPool = pool;
        this.connectionConfig = new OHConnectionConfiguration(conf);
        this.closed = false;
    }

    @Override
    public Configuration getConfiguration() {
        return this.conf;
    }

    private ExecutorService getBatchPool() {
        if (batchPool == null) {
            synchronized (this) {
                if (batchPool == null) {
                    this.batchPool = getThreadPool(
                        conf.getInt("hbase.hconnection.threads.max", 256),
                        conf.getInt("hbase.hconnection.threads.core", 256), "-shared-", null);
                }
            }
        }
        return this.batchPool;
    }

    private ExecutorService getThreadPool(int maxThreads, int coreThreads, String nameHint,
                                          BlockingQueue<Runnable> passedWorkQueue) {
        // shared HTable thread executor not yet initialized
        if (maxThreads == 0) {
            maxThreads = Runtime.getRuntime().availableProcessors() * 8;
        }
        if (coreThreads == 0) {
            coreThreads = Runtime.getRuntime().availableProcessors() * 8;
        }
        long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
        BlockingQueue<Runnable> workQueue = passedWorkQueue;
        if (workQueue == null) {
            workQueue = new LinkedBlockingQueue<Runnable>(
                maxThreads
                        * conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
                            HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
        }
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime,
            TimeUnit.SECONDS, workQueue, Threads.newDaemonThreadFactory(toString() + nameHint));
        tpe.allowCoreThreadTimeOut(true);
        return tpe;
    }

    @Override
    public HTableInterface getTable(TableName tableName) throws IOException {
        return getTable(tableName, getBatchPool());
    }

    @Override
    public HTableInterface getTable(TableName tableName, ExecutorService pool) throws IOException {
        return new OHTable(tableName, this, connectionConfig, pool);
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        return getBufferedMutator(new BufferedMutatorParams(tableName));
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
        if (params.getTableName() == null) {
            throw new IllegalArgumentException("TableName cannot be null.");
        }
        if (params.getPool() == null) {
            params.pool(HTable.getDefaultExecutor(getConfiguration()));
        }
        if (params.getWriteBufferSize() == BUFFERED_PARAM_UNSET) {
            params.writeBufferSize(connectionConfig.getWriteBufferSize());
        }
        if (params.getMaxKeyValueSize() == BUFFERED_PARAM_UNSET) {
            params.maxKeyValueSize(connectionConfig.getMaxKeyValueSize());
        }
        return new OHBufferedMutatorImpl(this, params);
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        throw new FeatureNotSupportedException("not supported yet'");
    }

    @Override
    public Admin getAdmin() throws IOException {
        throw new FeatureNotSupportedException("not supported yet'");
    }

    private void shutdownBatchPool(ExecutorService pool) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
        }
    }

    @Override
    public void close() {
        if (this.closed) {
            return;
        }
        if (this.cleanupPool && this.batchPool != null && !this.batchPool.isShutdown()) {
            shutdownBatchPool(this.batchPool);
        }
        this.closed = true;
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }

    @Override
    public void abort(final String msg, Throwable t) {
        if (t != null) {
            LOGGER.error(FATAL, msg, t);
        } else {
            LOGGER.error(FATAL, msg);
        }
        this.aborted = true;
        close();
    }

    @Override
    public boolean isAborted() {
        return this.aborted;
    }
}
