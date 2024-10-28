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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;

@InterfaceAudience.Private
public class OHBufferedMutatorImpl implements BufferedMutator {
    private static final Logger                   LOGGER                              = TableHBaseLoggerFactory
                                                                                          .getLogger(OHBufferedMutatorImpl.class);

    private final ExceptionListener               listener;

    private final OHTable                         ohTable;
    private final TableName                       tableName;
    private volatile Configuration                conf;
    private final OHConnectionConfiguration       connectionConfig;

    private final ConcurrentLinkedQueue<Mutation> asyncWriteBuffer                    = new ConcurrentLinkedQueue<Mutation>();
    private final AtomicLong                      currentAsyncBufferSize              = new AtomicLong(
                                                                                          0);

    private final AtomicLong                      firstRecordInBufferTimestamp        = new AtomicLong(
                                                                                          0);
    private final AtomicLong                      executedWriteBufferPeriodicFlushes  = new AtomicLong(
                                                                                          0);

    private final AtomicLong                      writeBufferPeriodicFlushTimeoutMs   = new AtomicLong(
                                                                                          0);
    private final AtomicLong                      writeBufferPeriodicFlushTimerTickMs = new AtomicLong(
                                                                                          MIN_WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS);
    private Timer                                 writeBufferPeriodicFlushTimer       = null;

    private final long                            writeBufferSize;
    private final int                             maxKeyValueSize;
    private final ExecutorService                 pool;
    private final AtomicInteger                   undealtMutationCount                = new AtomicInteger(
                                                                                          0);
    private final AtomicInteger                   rpcTimeout;
    private final AtomicInteger                   operationTimeout;
    private final boolean                         cleanupPoolOnClose;
    private volatile boolean                      closed                              = false;

    public OHBufferedMutatorImpl(OHConnectionImpl ohConnection, BufferedMutatorParams params)
                                                                                             throws IOException {
        if (ohConnection == null || ohConnection.isClosed()) {
            throw new IllegalArgumentException("Connection is null or closed.");
        }
        // init params in OHBufferedMutatorImpl
        this.tableName = params.getTableName();
        this.conf = ohConnection.getConfiguration();
        this.connectionConfig = ohConnection.getOHConnectionConfiguration();
        this.listener = params.getListener();
        if (params.getPool() == null) { // need to verify necessity
            this.pool = HTable.getDefaultExecutor(conf);
            this.cleanupPoolOnClose = true;
        } else {
            this.pool = params.getPool();
            this.cleanupPoolOnClose = false;
        }
        this.rpcTimeout = new AtomicInteger(
            params.getRpcTimeout() != OHConnectionImpl.BUFFERED_PARAM_UNSET ? params
                .getRpcTimeout() : connectionConfig.getRpcTimeout());
        this.operationTimeout = new AtomicInteger(
            params.getOperationTimeout() != OHConnectionImpl.BUFFERED_PARAM_UNSET ? params
                .getOperationTimeout() : connectionConfig.getOperationTimeout());

        long newPeriodicFlushTimeoutMs = params.getWriteBufferPeriodicFlushTimeoutMs() != OHConnectionImpl.BUFFERED_PARAM_UNSET ? params
            .getWriteBufferPeriodicFlushTimeoutMs() : connectionConfig
            .getWriteBufferPeriodicFlushTimeoutMs();
        long newPeriodicFlushTimeIntervalMs = params.getWriteBufferPeriodicFlushTimerTickMs() != OHConnectionImpl.BUFFERED_PARAM_UNSET ? params
            .getWriteBufferPeriodicFlushTimerTickMs() : connectionConfig
            .getWriteBufferPeriodicFlushTimerTickMs();
        this.setWriteBufferPeriodicFlush(newPeriodicFlushTimeoutMs, newPeriodicFlushTimeIntervalMs);

        this.writeBufferSize = params.getWriteBufferSize() != OHConnectionImpl.BUFFERED_PARAM_UNSET ? params
            .getWriteBufferSize() : connectionConfig.getWriteBufferSize();
        this.maxKeyValueSize = params.getMaxKeyValueSize() != OHConnectionImpl.BUFFERED_PARAM_UNSET ? params
            .getMaxKeyValueSize() : connectionConfig.getMaxKeyValueSize();

        // create an OHTable object to do batch work
        this.ohTable = new OHTable(tableName, ohConnection, connectionConfig, pool);
    }

    @Override
    public TableName getName() {
        return this.tableName;
    }

    @Override
    public Configuration getConfiguration() {
        return this.conf;
    }

    /**
     * Add the mutation into asyncWriteBuffer
     *
     * @param mutation - mutation operation
     */
    @Override
    public void mutate(Mutation mutation) throws IOException {
        mutate(Collections.singletonList(mutation));
    }

    /**
     * Add all mutations in List into asyncWriteBuffer
     *
     * @param mutations - mutation operations
     */
    @Override
    public void mutate(List<? extends Mutation> mutations) throws IOException {
        checkClose();
        if (mutations.isEmpty()) {
            return;
        }

        long toAddSize = 0;
        int toAddCount = 0;
        for (Mutation m : mutations) {
            validateOperation(m);
            toAddSize += m.heapSize();
            ++toAddCount;
        }

        if (currentAsyncBufferSize.get() == 0) {
            firstRecordInBufferTimestamp.set(System.currentTimeMillis());
        }
        undealtMutationCount.addAndGet(toAddCount);
        currentAsyncBufferSize.addAndGet(toAddSize);
        asyncWriteBuffer.addAll(mutations);

        execute(false);
    }

    private void checkClose() {
        if (closed) {
            throw new IllegalStateException("The BufferedMutator is closed.");
        }
    }

    /**
     * Check mutations in 2.x
     * @param mt - mutation operation
     */
    private void validateOperation(Mutation mt) throws IllegalArgumentException {
        if (mt == null) {
            throw new IllegalArgumentException("Mutation operation cannot be null.");
        }
        if (!(mt instanceof Put) && !(mt instanceof Delete)) {
            throw new IllegalArgumentException("Only support for Put and Delete for now.");
        }
        if (mt instanceof Put) {
            // family empty check is in validatePut
            HTable.validatePut((Put) mt, maxKeyValueSize);
            OHTable.checkFamilyViolation(mt.getFamilyCellMap().keySet(), true);
        } else {
            OHTable.checkFamilyViolation(mt.getFamilyCellMap().keySet(), false);
        }
    }

    /**
     * triggered to do periodic flush if reach the time limit
     * */
    public void timeTriggerForWriteBufferPeriodicFlush() {
        if (currentAsyncBufferSize.get() == 0) {
            return;
        }
        long now = System.currentTimeMillis();
        if (firstRecordInBufferTimestamp.get() + writeBufferPeriodicFlushTimeoutMs.get() > now) {
            // too soon to execute
            return;
        }
        try {
            executedWriteBufferPeriodicFlushes.incrementAndGet();
            flush();
        } catch (Exception e) {
            LOGGER.error("Errors occur during timeTriggerForWriteBufferPeriodicFlush: { "
                         + e.getMessage() + " }");
        }
    }

    /**
     * set time for periodic flush timer
     * @param timeoutMs control when to flush from collecting first mutation
     * @param timerTickMs control time interval to trigger the timer
     * */
    @Override
    public synchronized void setWriteBufferPeriodicFlush(long timeoutMs, long timerTickMs) {
        long originalTimeoutMs = this.writeBufferPeriodicFlushTimeoutMs.get();
        long originalTimeTickMs = this.writeBufferPeriodicFlushTimerTickMs.get();

        writeBufferPeriodicFlushTimeoutMs.set(Math.max(0, timeoutMs));
        writeBufferPeriodicFlushTimerTickMs.set(Math.max(
            MIN_WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS, timerTickMs));

        // if time parameters are updated, stop the old timer
        if (writeBufferPeriodicFlushTimeoutMs.get() != originalTimeoutMs
            || writeBufferPeriodicFlushTimerTickMs.get() != originalTimeTickMs) {
            if (writeBufferPeriodicFlushTimer != null) {
                writeBufferPeriodicFlushTimer.cancel();
                writeBufferPeriodicFlushTimer = null;
            }
        }

        if (writeBufferPeriodicFlushTimer == null && writeBufferPeriodicFlushTimeoutMs.get() > 0) {
            writeBufferPeriodicFlushTimer = new Timer(true);
            writeBufferPeriodicFlushTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    OHBufferedMutatorImpl.this.timeTriggerForWriteBufferPeriodicFlush();
                }
            }, this.writeBufferPeriodicFlushTimerTickMs.get(),
                this.writeBufferPeriodicFlushTimerTickMs.get());
        }
    }

    /**
     * Send the operations in the buffer to the servers. Does not wait for the server's answer. If
     * there is an error, either throw the error, or use the listener to deal with the error.
     *
     * @param flushAll - if true, force to commit all mutations in asyncWriteBuffer; else to commit only if
     * larger than writeBufferSize
     */
    private void execute(boolean flushAll) throws IOException {
        LinkedList<Mutation> execBuffer = new LinkedList<>();
        try {
            if (flushAll || currentAsyncBufferSize.get() > writeBufferSize) {
                Mutation m;
                int dealtCount = 0;
                while ((m = asyncWriteBuffer.poll())!= null) {
                    execBuffer.add(m);
                    long size = m.heapSize();
                    currentAsyncBufferSize.addAndGet(-size);
                    ++dealtCount;
                }
                undealtMutationCount.addAndGet(-dealtCount);
            }

            if (execBuffer.isEmpty()) {
                return;
            }
            Object[] results = new Object[execBuffer.size()];
            ohTable.batch(execBuffer, results);
            // if commit all successfully, clean execBuffer
            execBuffer.clear();
        } catch (Exception ex) {
            LOGGER.error(LCD.convert("01-00026"), ex);
            if (ex.getCause() instanceof RetriesExhaustedWithDetailsException) {
                LOGGER.error(tableName + ": One or more of the operations have failed after retries.");
                RetriesExhaustedWithDetailsException retryException = (RetriesExhaustedWithDetailsException) ex.getCause();
                // recollect failed mutations
                execBuffer.clear();
                for (int i = 0; i < retryException.getNumExceptions(); ++i) {
                    execBuffer.add((Mutation) retryException.getRow(i));
                }
                if (listener != null) {
                    listener.onException(retryException, this);
                } else {
                    throw retryException;
                }
            } else {
                LOGGER.error("Errors unrelated to operations occur during mutation operation", ex);
                throw ex;
            }
        } finally {
            for (Mutation mutation : execBuffer) {
                long size = mutation.heapSize();
                currentAsyncBufferSize.addAndGet(size);
                asyncWriteBuffer.add(mutation);
                undealtMutationCount.incrementAndGet();
            }
        }
    }

    /**
     * reset the time parameters and cancel the timer (if exists)
     * */
    @Override
    public void disableWriteBufferPeriodicFlush() {
        setWriteBufferPeriodicFlush(0, MIN_WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        // reset timeout, timeTick and Timer
        disableWriteBufferPeriodicFlush();
        try {
            execute(true);
        } finally {
            if (cleanupPoolOnClose) {
                // the pool in ObTableClient will be shut down too
                this.pool.shutdown();
                try {
                    if (!pool.awaitTermination(600, TimeUnit.SECONDS)) {
                        LOGGER
                            .warn("close() failed to terminate pool after 10 minutes. Abandoning pool.");
                    }
                } catch (InterruptedException e) {
                    LOGGER.warn("waitForTermination interrupted");
                    Thread.currentThread().interrupt();
                }
            }
            closed = true;
        }
    }

    /**
     * Force to commit all operations
     */
    @Override
    public void flush() throws IOException {
        checkClose();
        execute(true);
    }

    @Override
    public long getWriteBufferPeriodicFlushTimeoutMs() {
        return writeBufferPeriodicFlushTimeoutMs.get();
    }

    @Override
    public long getWriteBufferPeriodicFlushTimerTickMs() {
        return writeBufferPeriodicFlushTimerTickMs.get();
    }

    @Override
    public long getWriteBufferSize() {
        return this.writeBufferSize;
    }

    @Override
    public void setRpcTimeout(int rpcTimeout) {
        this.rpcTimeout.set(rpcTimeout);
        this.ohTable.setRpcTimeout(rpcTimeout);
    }

    @Override
    public void setOperationTimeout(int operationTimeout) {
        this.operationTimeout.set(operationTimeout);
        this.ohTable.setOperationTimeout(operationTimeout);
    }

    /**
     * Count the mutations which haven't been processed.
     */
    @VisibleForTesting
    public int size() {
        return undealtMutationCount.get();
    }

    @VisibleForTesting
    public ExecutorService getPool() {
        return pool;
    }

    @VisibleForTesting
    protected long getExecutedWriteBufferPeriodicFlushes() {
        return executedWriteBufferPeriodicFlushes.get();
    }
}
