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
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableBatchOperation;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.alipay.oceanbase.hbase.util.TableHBaseLoggerFactory.LCD;
import static com.alipay.oceanbase.rpc.ObGlobal.*;

@InterfaceAudience.Private
public class OHBufferedMutatorImpl implements BufferedMutator {
    private static final Logger           LOGGER                 = TableHBaseLoggerFactory
                                                                     .getLogger(OHBufferedMutatorImpl.class);

    private final ExceptionListener       listener;

    private final TableName               tableName;
    private volatile Configuration        conf;

    private OHTable                       ohTable;
    @VisibleForTesting
    final ConcurrentLinkedQueue<Mutation> asyncWriteBuffer       = new ConcurrentLinkedQueue<Mutation>();
    @VisibleForTesting
    AtomicLong                            currentAsyncBufferSize = new AtomicLong(0);

    private long                          writeBufferSize;
    private final int                     maxKeyValueSize;
    private boolean                       closed                 = false;
    private final ExecutorService         pool;
    private int                           rpcTimeout;
    private int                           operationTimeout;
    private static final long             OB_VERSION_4_2_5_1     = calcVersion(4, (short) 2,
                                                                     (byte) 5, (byte) 1);

    public OHBufferedMutatorImpl(OHConnectionImpl ohConnection, BufferedMutatorParams params,
                                 OHTable ohTable) throws IOException {
        if (ohConnection == null || ohConnection.isClosed()) {
            throw new IllegalArgumentException("Connection is null or closed.");
        }
        // init params in OHBufferedMutatorImpl
        this.tableName = params.getTableName();
        this.conf = ohConnection.getConfiguration();
        this.listener = params.getListener();

        OHConnectionConfiguration connectionConfig = ohConnection.getOHConnectionConfiguration();
        this.pool = params.getPool();
        this.rpcTimeout = connectionConfig.getRpcTimeout();
        this.operationTimeout = connectionConfig.getOperationTimeout();

        this.writeBufferSize = params.getWriteBufferSize() != OHConnectionImpl.BUFFERED_PARAM_UNSET ? params
            .getWriteBufferSize() : connectionConfig.getWriteBufferSize();
        this.maxKeyValueSize = params.getMaxKeyValueSize() != OHConnectionImpl.BUFFERED_PARAM_UNSET ? params
            .getMaxKeyValueSize() : connectionConfig.getMaxKeyValueSize();

        // create an OHTable object to do batch work
        if (ohTable != null) {
            this.ohTable = ohTable;
        } else {
            this.ohTable = new OHTable(tableName, ohConnection, connectionConfig, pool);
        }
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
        if (closed) {
            throw new IllegalStateException("Cannot put when the BufferedMutator is closed.");
        }
        if (mutations.isEmpty()) {
            return;
        }

        long toAddSize = 0;
        for (Mutation m : mutations) {
            validateOperation(m);
            toAddSize += m.heapSize();
        }
        currentAsyncBufferSize.addAndGet(toAddSize);
        asyncWriteBuffer.addAll(mutations);

        if (currentAsyncBufferSize.get() > writeBufferSize) {
            batchExecute(false);
        }
    }

    /**
     * Check whether the mutation is Put or Delete in 1.x
     * @param mt - mutation operation
     */
    private void validateOperation(Mutation mt) throws IllegalArgumentException {
        if (mt == null) {
            throw new IllegalArgumentException("Mutation operation cannot be null");
        }
        if (!(mt instanceof Put) && !(mt instanceof Delete)) {
            throw new IllegalArgumentException("Only support for Put and Delete for now.");
        }
        if (mt instanceof Put) {
            // family empty check is in validatePut
            OHTable.validatePut((Put) mt, maxKeyValueSize);
            if (isMultiFamilySupport()) {
                OHTable.checkFamilyViolation(mt.getFamilyMap().keySet(), true);
            } else {
                OHTable.checkFamilyViolationForOneFamily(mt.getFamilyMap().keySet());
            }
        } else {
            if (isMultiFamilySupport()) {
                OHTable.checkFamilyViolation(mt.getFamilyMap().keySet(), false);
            } else {
                OHTable.checkFamilyViolationForOneFamily(mt.getFamilyMap().keySet());
            }
        }
    }

    /**
     * This execute only supports for server version of 4_3_5.
     * Send the operations in the buffer to the servers. Does not wait for the server's answer. If
     * there is an error, either throw the error, or use the listener to deal with the error.
     *
     * @param flushAll - if true, sends all the writes and wait for all of them to finish before
     *        returning.
     */
    private void batchExecute(boolean flushAll) throws IOException {
        LinkedList<Mutation> execBuffer = new LinkedList<>();
        long dequeuedSize = 0L;
        try {
            Mutation m;
            while ((writeBufferSize <= 0 || dequeuedSize < (writeBufferSize * 2) || flushAll)
                && (m = asyncWriteBuffer.poll()) != null) {
                execBuffer.add(m);
                long size = m.heapSize();
                currentAsyncBufferSize.addAndGet(-size);
                dequeuedSize += size;
            }

            if (execBuffer.isEmpty()) {
                return;
            }
            ohTable.batch(execBuffer);
            // if commit all successfully, clean execBuffer
            execBuffer.clear();
        } catch (Exception ex) {
            if (ex.getCause() instanceof RetriesExhaustedWithDetailsException) {
                LOGGER.error(LCD.convert("01-00011"), tableName.getNameAsString()
                        + ": One or more of the operations have failed after retries.", ex.getCause());
                RetriesExhaustedWithDetailsException retryException = (RetriesExhaustedWithDetailsException) ex.getCause();
                // recollect mutations and log error information
                execBuffer.clear();
                for (int i = 0;  i < retryException.getNumExceptions(); ++i) {
                    Row failedOp = retryException.getRow(i);
                    execBuffer.add((Mutation) failedOp);
                    LOGGER.error(LCD.convert("01-00011"), failedOp, tableName.getNameAsString(),
                            currentAsyncBufferSize.get(), retryException.getCause(i));
                }
                if (listener != null) {
                    listener.onException(retryException, this);
                } else {
                    throw retryException;
                }
            } else {
                LOGGER.error(LCD.convert("01-00011"), tableName.getNameAsString()
                        + ": Errors unrelated to operations occur during mutation operation", ex);
                throw ex;
            }
        } finally {
            for (Mutation mutation : execBuffer) {
                long size = mutation.heapSize();
                currentAsyncBufferSize.addAndGet(size);
                asyncWriteBuffer.add(mutation);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            flush();
        } finally {
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
            closed = true;
        }
    }

    @Deprecated
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        this.writeBufferSize = writeBufferSize;
        if (currentAsyncBufferSize.get() > writeBufferSize) {
            flush();
        }
    }

    private ObTableBatchOperation buildObTableBatchOperation(List<? extends Mutation> execBuffer) {
        List<KeyValue> keyValueList = new LinkedList<>();
        for (Mutation mutation : execBuffer) {
            for (Map.Entry<byte[], List<KeyValue>> entry : mutation.getFamilyMap().entrySet()) {
                keyValueList.addAll(entry.getValue());
            }
        }
        return OHTable.buildObTableBatchOperation(keyValueList, false, null);
    }

    /**
     * Only 4_2_5 BP1 - 4_3_0 and after 4_3_4 support multi-cf
     * */
    boolean isMultiFamilySupport() {
        return (OB_VERSION >= OB_VERSION_4_2_5_1 && OB_VERSION < OB_VERSION_4_3_0_0)
               || (OB_VERSION >= OB_VERSION_4_3_4_0);
    }

    /**
     * Force to commit all operations
     * do not care whether the pool is shut down or this BufferedMutator is closed
     */
    @Override
    public void flush() throws IOException {
        batchExecute(true);
    }

    @Override
    public long getWriteBufferSize() {
        return this.writeBufferSize;
    }

    public void setRpcTimeout(int rpcTimeout) {
        this.rpcTimeout = rpcTimeout;
        this.ohTable.setRpcTimeout(rpcTimeout);
    }

    public void setOperationTimeout(int operationTimeout) {
        this.operationTimeout = operationTimeout;
        this.ohTable.setOperationTimeout(operationTimeout);
    }

    public long getCurrentBufferSize() {
        return currentAsyncBufferSize.get();
    }

    @Deprecated
    public List<Row> getWriteBuffer() {
        return Arrays.asList(asyncWriteBuffer.toArray(new Row[0]));
    }
}
