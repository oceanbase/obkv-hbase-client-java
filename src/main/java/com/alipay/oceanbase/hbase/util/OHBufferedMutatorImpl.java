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
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableBatchOperation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableBatchOperationRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableBatchOperationResult;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
    private ObTableClient                 obTableClient;
    @VisibleForTesting
    final ConcurrentLinkedQueue<Mutation> asyncWriteBuffer       = new ConcurrentLinkedQueue<Mutation>();
    @VisibleForTesting
    AtomicLong                            currentAsyncBufferSize = new AtomicLong(0);
    private AtomicReference<Class<?>>     type                   = new AtomicReference<>(null);

    private long                          writeBufferSize;
    private final int                     maxKeyValueSize;
    private boolean                       closed                 = false;
    private final ExecutorService         pool;
    private int                           rpcTimeout;
    private int                           operationTimeout;
    private static final long             OB_VERSION_4_3_5_0     = calcVersion(4, (short) 3, (byte) 5, (byte) 0);

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

        if (isBatchSupport()) {
            // create an OHTable object to do batch work
            if (ohTable != null) {
                this.ohTable = ohTable;
            } else {
                this.ohTable = new OHTable(tableName, ohConnection, connectionConfig, pool);
            }
        } else {
            // create an ObTableClient object to execute batch operation request
            this.obTableClient = ObTableClientManager.getOrCreateObTableClient(connectionConfig);
            this.obTableClient.setRuntimeBatchExecutor(pool);
            this.obTableClient.setRpcExecuteTimeout(rpcTimeout);
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
        if (isBatchSupport()) {
            for (Mutation m : mutations) {
                validateOperation(m);
                toAddSize += m.heapSize();
            }

            currentAsyncBufferSize.addAndGet(toAddSize);
            asyncWriteBuffer.addAll(mutations);

            if (currentAsyncBufferSize.get() > writeBufferSize) {
                batchExecute(false);
            }
        } else {
            // check if every mutation's family is the same
            // check if mutations are the same type
            for (Mutation m : mutations) {
                validateOperation(m);
                Class<?> curType = m.getClass();
                // set the type of this BufferedMutator
                type.compareAndSet(null, curType);
                if (!type.get().equals(curType)) {
                    throw new IllegalArgumentException("Not support different type in one batch.");
                }
                toAddSize += m.heapSize();
            }

            currentAsyncBufferSize.addAndGet(toAddSize);
            asyncWriteBuffer.addAll(mutations);

            if (currentAsyncBufferSize.get() > writeBufferSize) {
                normalExecute(false);
            }
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
            OHTable.checkFamilyViolation(mt.getFamilyMap().keySet(), true);
        } else {
            OHTable.checkFamilyViolation(mt.getFamilyMap().keySet(), false);
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

    /**
     * This execute supports for server version below 4_3_5.
     * Send the operations in the buffer to the servers. Does not wait for the server's answer. If
     * there is an error, either throw the error, or use the listener to deal with the error.
     *
     * @param flushAll - if true, sends all the writes and wait for all of them to finish before
     *        returning.
     */
    private void normalExecute(boolean flushAll) throws IOException {
        LinkedList<Mutation> execBuffer = new LinkedList<>();
        ObTableBatchOperationRequest request = null;
        // namespace n1, n1:table_name
        // namespace default, table_name
        String tableNameString = tableName.getNameAsString();
        try {
            long dequeuedSize = 0L;
            Mutation m;
            while ((writeBufferSize <= 0 || dequeuedSize < (writeBufferSize * 2) || flushAll)
                    && (m = asyncWriteBuffer.poll()) != null) {
                execBuffer.add(m);
                long size = m.heapSize();
                currentAsyncBufferSize.addAndGet(-size);
                dequeuedSize += size;
            }
            // in concurrent situation, asyncWriteBuffer may be empty here
            // for other threads flush all buffer
            if (execBuffer.isEmpty()) {
                return;
            }
            try{
                // for now, operations' family is the same
                byte[] family = execBuffer.getFirst().getFamilyMap().firstKey();
                ObTableBatchOperation batch = buildObTableBatchOperation(execBuffer);
                // table_name$cf_name
                String targetTableName = OHTable.getTargetTableName(tableNameString, Bytes.toString(family), conf);
                request = OHTable.buildObTableBatchOperationRequest(batch, targetTableName);
            } catch (Exception ex) {
                LOGGER.error(LCD.convert("01-00011"), tableName.getNameAsString()
                        + ": Errors unrelated to operations occur before mutation operation", ex);
                throw new ObTableUnexpectedException(tableName.getNameAsString() + ": Errors occur before mutation operation", ex);
            }
            try {
                ObTableBatchOperationResult result = (ObTableBatchOperationResult) obTableClient.execute(request);
            } catch (Exception ex) {
                LOGGER.debug(LCD.convert("01-00011"), tableName.getNameAsString() +
                        ": Errors occur during mutation operation", ex);
                m = null;
                try {
                    // retry every single operation
                    while (!execBuffer.isEmpty()) {
                        // poll elements from execBuffer to recollect remaining operations
                        m = execBuffer.poll();
                        byte[] family = m.getFamilyMap().firstKey();
                        ObTableBatchOperation batch = buildObTableBatchOperation(Collections.singletonList(m));
                        String targetTableName = OHTable.getTargetTableName(tableNameString, Bytes.toString(family), conf);
                        request = OHTable.buildObTableBatchOperationRequest(batch, targetTableName);
                        ObTableBatchOperationResult result = (ObTableBatchOperationResult) obTableClient.execute(request);
                    }
                } catch (Exception newEx) {
                    if (m != null) {
                        execBuffer.addFirst(m);
                    }
                    // if retry fails, only recollect remaining operations
                    while(!execBuffer.isEmpty()) {
                        m = execBuffer.poll();
                        long size = m.heapSize();
                        asyncWriteBuffer.add(m);
                        currentAsyncBufferSize.addAndGet(size);
                    }
                    throw newEx;
                }
            }
        } catch (Exception ex) {
            LOGGER.error(LCD.convert("01-00011"), tableName.getNameAsString() +
                    ": Errors occur during mutation operation", ex);
            // if the cause is illegal argument, directly throw to user
            if (ex instanceof ObTableUnexpectedException) {
                throw (ObTableUnexpectedException) ex;
            }
            // TODO: need to collect error information and actions in old version
            // TODO: maybe keep in ObTableBatchOperationResult
            List<Throwable> throwables = new ArrayList<Throwable>();
            List<Row> actions = new ArrayList<Row>();
            List<String> addresses = new ArrayList<String>();
            throwables.add(ex);
            RetriesExhaustedWithDetailsException error = new RetriesExhaustedWithDetailsException(
                    new ArrayList<Throwable>(throwables),
                    new ArrayList<Row>(actions), new ArrayList<String>(addresses));
            if (listener == null) {
                throw error;
            } else {
                listener.onException(error, this);
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

    boolean isBatchSupport() {
        return OB_VERSION >= OB_VERSION_4_3_5_0;
    }

    /**
     * Force to commit all operations
     * do not care whether the pool is shut down or this BufferedMutator is closed
     */
    @Override
    public void flush() throws IOException {
        if (isBatchSupport()) {
            batchExecute(true);
        } else {
            normalExecute(true);
        }
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
