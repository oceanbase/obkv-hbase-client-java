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
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.*;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import sun.awt.image.ImageWatched;

import javax.ws.rs.PUT;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static com.alipay.oceanbase.hbase.constants.OHConstants.DEFAULT_HBASE_HTABLE_TEST_LOAD_SUFFIX;
import static com.alipay.oceanbase.hbase.util.Preconditions.checkArgument;
import static com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperation.getInstance;
import static com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType.*;
import static com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType.DEL;
import static org.apache.commons.lang.StringUtils.isBlank;

@InterfaceAudience.Private
public class OHBufferedMutatorImpl implements BufferedMutator {
    private static final Logger LOGGER     = TableHBaseLoggerFactory
            .getLogger(OHConnectionImpl.class);

    private static final int  BUFFERED_PARAM_UNSET = -1;

    private final ExceptionListener listener;

    protected final ObTableClient obTableClient;
    private final TableName tableName;
    private volatile Configuration conf;
    private final OHConnectionConfiguration connectionConfig;

    @VisibleForTesting
    final ConcurrentLinkedQueue<Mutation> asyncWriteBuffer = new ConcurrentLinkedQueue<Mutation>();
    @VisibleForTesting
    AtomicLong currentAsyncBufferSize = new AtomicLong(0);

    private long writeBufferSize;
    private final int maxKeyValueSize;
    private boolean closed = false;
    private final ExecutorService pool;
    private int rpcTimeout;
    private int operationTimeout;
    private final boolean cleanupPoolOnClose;

    public OHBufferedMutatorImpl(OHConnectionImpl ohConnection, BufferedMutatorParams params) throws IOException {
        if (ohConnection == null || ohConnection.isClosed()) {
            throw new IllegalArgumentException("Connection is null or closed.");
        }
        // create a ObTableClient to do rpc operations
        this.obTableClient = ObTableClientManager.getOrCreateObTableClient(ohConnection.getOHConnectionConfiguration());

        // init params in OHBufferedMutatorImpl:
        // TableName + pool + Configuration + listener + writeBufferSize + maxKeyValueSize + rpcTimeout + operationTimeout
        this.tableName = params.getTableName();
        this.conf = ohConnection.getConfiguration();
        this.connectionConfig = ohConnection.getOHConnectionConfiguration();
        this.listener = params.getListener();

        if (params.getPool() == null) {
            this.pool = HTable.getDefaultExecutor(ohConnection.getConfiguration());
            this.cleanupPoolOnClose = true;
        } else {
            this.pool = params.getPool();
            this.cleanupPoolOnClose = false;
        }

        this.writeBufferSize = params.getWriteBufferSize() != BUFFERED_PARAM_UNSET ?
                params.getWriteBufferSize() : connectionConfig.getWriteBufferSize();
        this.maxKeyValueSize = params.getMaxKeyValueSize() != BUFFERED_PARAM_UNSET ?
                params.getMaxKeyValueSize() : connectionConfig.getMaxKeyValueSize();
        this.rpcTimeout = connectionConfig.getRpcTimeout();
        this.operationTimeout = connectionConfig.getOperationTimeout();
    }

    @Override
    public TableName getName() {
        return this.tableName;
    }

    @Override
    public Configuration getConfiguration() {
        return this.conf;
    }

    @Override
    public void mutate(Mutation mutation) throws InterruptedIOException,
    RetriesExhaustedWithDetailsException {
        // convert mutation into list, use the interface below
        mutate(Collections.singletonList(mutation));
    }

    // only support for Put and Delete for 1.x
    @Override
    public void mutate(List<? extends Mutation> mutations) throws InterruptedIOException,
            RetriesExhaustedWithDetailsException {
        // add the mutations into writeAsyncBuffer
        // atomically add size of mutations into currentWriteBufferSize
        // do the flush/backgroundFlushCommits if currentWriteBufferSize > writeBufferSize
        if (closed) {
            throw new IllegalStateException("Cannot put when the BufferedMutator is closed.");
        }

        long toAddSize = 0;
        // check if every mutation's family is the same
        if (!validateSameFamily(mutations)) {
            throw new IllegalStateException("Family should keep the same in one batch.");
        }
        for (Mutation m : mutations) {
            if (!validateInsUpAndDelete(m)) {
                throw new IllegalArgumentException("Only support for Put and Delete for now.");
            }
            if (m instanceof Put) {
                HTable.validatePut((Put) m, maxKeyValueSize);
            }
            toAddSize += m.heapSize();
        }

        currentAsyncBufferSize.addAndGet(toAddSize);
        asyncWriteBuffer.addAll(mutations);

        asyncExecute(false);
    }

    boolean validateInsUpAndDelete(Mutation mt) {
        if (!(mt instanceof Put) && !(mt instanceof Delete)) {
            return false;
        }
        return true;
    }

    boolean validateSameFamily(List<? extends Mutation> mutations) {
        for (Mutation mutation : mutations) {
            if (mutation.getFamilyMap().keySet() == null
                    || mutation.getFamilyMap().keySet().size() == 0) {
                throw new IllegalArgumentException("Family is not provided in batch operations.");
            }
            if (mutation.getFamilyMap().keySet().size() > 1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Send the operations in the buffer to the servers. Does not wait for the server's answer. If
     * the is an error (max retried reach from a previous flush or bad operation), it tries to send
     * all operations in the buffer and sends an exception.
     *
     * @param flushAll - if true, sends all the writes and wait for all of them to finish before
     *        returning.
     */
    private void asyncExecute(boolean flushAll) throws
            InterruptedIOException,
            RetriesExhaustedWithDetailsException {
        while (true) {
            if (!flushAll && currentAsyncBufferSize.get() <= writeBufferSize) {
                // There is the room to accept more mutations.
                break;
            }
            try {
                // namespace n1, n1:table_name
                // namespace default, table_name
                String tableNameString = tableName.getNameAsString();
                Map.Entry<byte[], List<KeyValue>> entry = asyncWriteBuffer.peek().getFamilyMap().entrySet().iterator().next();
                byte[] family = entry.getKey();
                ObTableBatchOperation batch = buildObTableBatchOperation(asyncWriteBuffer);
                String targetTableName = getTargetTableName(tableNameString, Bytes.toString(family));
                ObTableBatchOperationRequest request = buildObTableBatchOperationRequest(batch, targetTableName, pool);
                ObTableBatchOperationResult result = (ObTableBatchOperationResult) obTableClient.execute(request);
            } catch (Exception ex) {

            }
        }

    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            asyncExecute(true);
        } finally {
            if (cleanupPoolOnClose) {
                this.pool.shutdown();
                try {
                    if (!pool.awaitTermination(600, TimeUnit.SECONDS)) {
                        LOGGER.warn("close() failed to terminate pool after 10 minutes. Abandoning pool.");
                    }
                } catch (InterruptedException e) {
                    LOGGER.warn("waitForTermination interrupted");
                    Thread.currentThread().interrupt();
                }
            }
            closed = true;
        }
    }

    @Override
    public void flush() throws IOException {
        if (closed) {
            throw new IllegalStateException("Cannot put when the BufferedMutator is closed.");
        }
        asyncExecute(true);
    }

    @Override
    public long getWriteBufferSize() {
        return this.writeBufferSize;
    }

    private ObTableBatchOperation buildObTableBatchOperation(ConcurrentLinkedQueue<Mutation> asyncWriteBuffer) {
        LinkedList<Mutation> execBuffer = new LinkedList<>();
        Mutation m;
        while ((m = asyncWriteBuffer.poll()) != null) {
            execBuffer.add(m);
            long size = m.heapSize();
            currentAsyncBufferSize.addAndGet(-size);
        }
        List<KeyValue> keyValueList = new LinkedList<>();
        for (Mutation mutation : execBuffer) {
            checkFamilyViolation(mutation.getFamilyMap().keySet());
            for (Map.Entry<byte[], List<KeyValue>> entry : mutation.getFamilyMap().entrySet()) {
                keyValueList.addAll(entry.getValue());
            }
        }
        return buildObTableBatchOperation(keyValueList, false, null);
    }

    private ObTableBatchOperation buildObTableBatchOperation(List<KeyValue> keyValueList,
                                                             boolean putToAppend,
                                                             List<byte[]> qualifiers) {
        ObTableBatchOperation batch = new ObTableBatchOperation();
        for (KeyValue kv : keyValueList) {
            if (qualifiers != null) {
                qualifiers.add(kv.getQualifier());
            }
            batch.addTableOperation(buildObTableOperation(kv, putToAppend));
        }
        batch.setSameType(true);
        batch.setSamePropertiesNames(true);
        return batch;
    }

    private ObTableOperation buildObTableOperation(KeyValue kv, boolean putToAppend) {
        KeyValue.Type kvType = KeyValue.Type.codeToType(kv.getType());
        switch (kvType) {
            case Put:
                ObTableOperationType operationType;
                if (putToAppend) {
                    operationType = APPEND;
                } else {
                    operationType = INSERT_OR_UPDATE;
                }
                return getInstance(operationType,
                        new Object[] { kv.getRow(), kv.getQualifier(), kv.getTimestamp() }, V_COLUMNS,
                        new Object[] { kv.getValue() });
            case Delete:
                return getInstance(DEL,
                        new Object[] { kv.getRow(), kv.getQualifier(), kv.getTimestamp() }, null, null);
            case DeleteColumn:
                return getInstance(DEL,
                        new Object[] { kv.getRow(), kv.getQualifier(), -kv.getTimestamp() }, null, null);
            case DeleteFamily:
                return getInstance(DEL, new Object[] { kv.getRow(), null, -kv.getTimestamp() },
                        null, null);
            default:
                throw new IllegalArgumentException("illegal mutation type " + kvType);
        }
    }

    private ObTableBatchOperationRequest buildObTableBatchOperationRequest(ObTableBatchOperation obTableBatchOperation,
                                                                           String targetTableName,
                                                                           ExecutorService pool) {
        ObTableBatchOperationRequest request = new ObTableBatchOperationRequest();
        request.setTableName(targetTableName);
        request.setReturningAffectedRows(true);
        request.setEntityType(ObTableEntityType.HKV);
        request.setBatchOperation(obTableBatchOperation);
        request.setPool(pool);
        return request;
    }

    private void checkFamilyViolation(Collection<byte[]> families) {
        if (families == null || families.size() == 0) {
            throw new FeatureNotSupportedException("family is empty.");
        }

        if (families.size() > 1) {
            throw new FeatureNotSupportedException("multi family is not supported yet.");
        }

        for (byte[] family : families) {
            if (family == null || family.length == 0) {
                throw new IllegalArgumentException("family is empty");
            }
            if (isBlank(Bytes.toString(family))) {
                throw new IllegalArgumentException("family is blank");
            }
        }

    }

    private String getTargetTableName(String tableNameString, String familyString) {
        checkArgument(tableNameString != null, "tableNameString is null");
        checkArgument(familyString != null, "familyString is null");
        if (conf.getBoolean(HBASE_HTABLE_TEST_LOAD_ENABLE, false)) {
            return getTestLoadTargetTableName(tableNameString, familyString);
        }
        return getNormalTargetTableName(tableNameString, familyString);
    }

    private String getTestLoadTargetTableName(String tableNameString, String familyString) {
        String suffix = conf.get(HBASE_HTABLE_TEST_LOAD_SUFFIX,
                DEFAULT_HBASE_HTABLE_TEST_LOAD_SUFFIX);
        return tableNameString + suffix + "$" + familyString;
    }

    private String getNormalTargetTableName(String tableNameString, String familyString) {
        return tableNameString + "$" + familyString;
    }
}
