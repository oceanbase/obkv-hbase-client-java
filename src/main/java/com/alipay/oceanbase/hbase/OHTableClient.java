/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2022 OceanBase Group
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

import com.alipay.oceanbase.hbase.core.Lifecycle;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

public class OHTableClient implements HTableInterface, Lifecycle {
    private byte[]              tableName;
    private String              tableNameString;
    private ReentrantLock       lock        = new ReentrantLock();
    private OHTable             ohTable;
    private volatile boolean    initialized = false;
    private final Configuration conf;
    private ExecutorService     runtimeBatchExecutor;

    public void setRuntimeBatchExecutor(ExecutorService runtimeBatchExecutor) {
        this.runtimeBatchExecutor = runtimeBatchExecutor;
    }

    /**
     * The constructor.
     *
     * <p> NOTE: Required parameters in conf:</p>
     * <pre>
     *      Configuration conf = new Configuration();
     *      conf.set(HBASE_OCEANBASE_PARAM_URL, "http://param_url.com?database=test");
     *      conf.set(HBASE_OCEANBASE_FULL_USER_NAME, "username");
     *      conf.set(HBASE_OCEANBASE_PASSWORD, "password");
     *      conf.set(HBASE_OCEANBASE_SYS_USER_NAME, "sys_user_name");
     *      conf.set(HBASE_OCEANBASE_SYS_PASSWORD, "sys_password");
     *</pre>
     *
     * @param tableNameString table name
     * @param conf configure
     */
    public OHTableClient(String tableNameString, Configuration conf) {
        this.tableNameString = tableNameString;
        this.tableName = tableNameString.getBytes();
        this.conf = conf;
    }

    /**
     * Initial the OHTableClient, must be init before use.
     * @throws Exception if init OHTable failed
     */
    @Override
    public void init() throws Exception {
        if (!initialized) {
            lock.lock();
            try {
                if (!initialized) {
                    ohTable = new OHTable(conf, tableNameString);
                    initialized = true;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (initialized) {
            lock.lock();
            try {
                if (initialized) {
                    if (ohTable != null) {
                        ohTable.close();
                    }
                }
            } finally {
                initialized = false;
                lock.unlock();
            }
        }
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        checkStatus();
        return ohTable.coprocessorService(row);
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service,
                                                                    byte[] startKey, byte[] endKey,
                                                                    Batch.Call<T, R> callable)
                                                                                              throws ServiceException,
                                                                                              Throwable {
        checkStatus();
        return ohTable.coprocessorService(service, startKey, endKey, callable);
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
                                                          byte[] endKey, Batch.Call<T, R> callable,
                                                          Batch.Callback<R> callback)
                                                                                     throws ServiceException,
                                                                                     Throwable {
        checkStatus();
        ohTable.coprocessorService(service, startKey, endKey, callable, callback);
    }

    private void checkStatus() throws IllegalStateException {
        if (!initialized) {
            throw new IllegalStateException("tableName " + tableNameString + " is not initialized");
        }
    }

    @Override
    public void setAutoFlush(boolean autoFlush) {
        checkStatus();
        ohTable.setAutoFlush(autoFlush);
    }

    @Override
    public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
        checkStatus();
        ohTable.setAutoFlush(autoFlush, clearBufferOnFail);
    }

    @Override
    public void setAutoFlushTo(boolean autoFlush) {
        checkStatus();
        ohTable.setAutoFlushTo(autoFlush);
    }

    @Override
    public long getWriteBufferSize() {
        checkStatus();
        return ohTable.getWriteBufferSize();
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        checkStatus();
        ohTable.setWriteBufferSize(writeBufferSize);
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor,
                                                                      Message request,
                                                                      byte[] startKey,
                                                                      byte[] endKey,
                                                                      R responsePrototype)
                                                                                          throws ServiceException,
                                                                                          Throwable {
        checkStatus();
        return ohTable.batchCoprocessorService(methodDescriptor, request, startKey, endKey,
            responsePrototype);
    }

    @Override
    public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor,
                                                            Message request, byte[] startKey,
                                                            byte[] endKey, R responsePrototype,
                                                            Batch.Callback<R> callback)
                                                                                       throws ServiceException,
                                                                                       Throwable {
        checkStatus();
        ohTable.batchCoprocessorService(methodDescriptor, request, startKey, endKey,
            responsePrototype, callback);
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
                                  CompareFilter.CompareOp compareOp, byte[] value,
                                  RowMutations mutations) throws IOException {
        checkStatus();
        return ohTable.checkAndMutate(row, family, qualifier, compareOp, value, mutations);
    }

    @Override
    public void setOperationTimeout(int i) {
        checkStatus();
        ohTable.setOperationTimeout(i);
    }

    @Override
    public int getOperationTimeout() {
        checkStatus();
        return ohTable.getOperationTimeout();
    }

    @Override
    public void setRpcTimeout(int i) {
        checkStatus();
        ohTable.setRpcTimeout(i);
    }

    @Override
    public int getRpcTimeout() {
        checkStatus();
        return ohTable.getRpcTimeout();
    }

    @Override
    public byte[] getTableName() {
        return tableName;
    }

    @Override
    public TableName getName() {
        return ohTable.getName();
    }

    @Override
    public Configuration getConfiguration() {
        checkStatus();
        return ohTable.getConfiguration();
    }

    // Not support.
    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        checkStatus();
        return ohTable.getTableDescriptor();
    }

    @Override
    public boolean exists(Get get) throws IOException {
        checkStatus();
        return ohTable.exists(get);
    }

    @Override
    public boolean[] existsAll(List<Get> list) throws IOException {
        checkStatus();
        return ohTable.existsAll(list);
    }

    @Override
    public Boolean[] exists(List<Get> gets) throws IOException {
        checkStatus();
        return ohTable.exists(gets);
    }

    // Not support.
    @Override
    public void batch(List<? extends Row> actions, Object[] results) throws IOException,
                                                                    InterruptedException {
        checkStatus();
        ohTable.batch(actions, results);
    }

    // Not support.
    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
        checkStatus();
        return ohTable.batch(actions);
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions, Object[] results,
                                  Batch.Callback<R> callback) throws IOException,
                                                             InterruptedException {
        checkStatus();
        ohTable.batchCallback(actions, results, callback);
    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback)
                                                                                              throws IOException,
                                                                                              InterruptedException {
        checkStatus();
        return ohTable.batchCallback(actions, callback);
    }

    @Override
    public Result get(Get get) throws IOException {
        checkStatus();
        return ohTable.get(get);
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        checkStatus();
        return ohTable.get(gets);
    }

    // Not support.
    @Override
    public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
        checkStatus();
        return ohTable.getRowOrBefore(row, family);
    }

    public List<ResultScanner> getScanners(Scan scan) throws IOException {
        checkStatus();
        return ohTable.getScanners(scan);
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        checkStatus();
        return ohTable.getScanner(scan);
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        checkStatus();
        return ohTable.getScanner(family);
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        checkStatus();
        return ohTable.getScanner(family, qualifier);
    }

    @Override
    public void put(Put put) throws IOException {
        checkStatus();
        ohTable.put(put);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        checkStatus();
        ohTable.put(puts);
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
                                                                                                  throws IOException {
        checkStatus();
        return ohTable.checkAndPut(row, family, qualifier, value, put);
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
                               CompareFilter.CompareOp compareOp, byte[] value, Put put)
                                                                                        throws IOException {
        checkStatus();
        return ohTable.checkAndPut(row, family, qualifier, compareOp, value, put);
    }

    @Override
    public void delete(Delete delete) throws IOException {
        checkStatus();
        ohTable.delete(delete);
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        checkStatus();
        ohTable.delete(deletes);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
                                  Delete delete) throws IOException {
        checkStatus();
        return ohTable.checkAndDelete(row, family, qualifier, value, delete);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
                                  CompareFilter.CompareOp compareOp, byte[] value, Delete delete)
                                                                                                 throws IOException {
        checkStatus();
        return ohTable.checkAndDelete(row, family, qualifier, compareOp, value, delete);
    }

    // Not support.
    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        checkStatus();
        ohTable.mutateRow(rm);
    }

    @Override
    public Result append(Append append) throws IOException {
        checkStatus();
        return ohTable.append(append);
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        checkStatus();
        return ohTable.increment(increment);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
                                                                                              throws IOException {
        checkStatus();
        return ohTable.incrementColumnValue(row, family, qualifier, amount);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
                                     Durability durability) throws IOException {
        checkStatus();
        return ohTable.incrementColumnValue(row, family, qualifier, amount, durability);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
                                     boolean writeToWAL) throws IOException {
        checkStatus();
        return ohTable.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
    }

    @Override
    public boolean isAutoFlush() {
        checkStatus();
        return ohTable.isAutoFlush();
    }

    @Override
    public void flushCommits() throws IOException {
        checkStatus();
        ohTable.flushCommits();
    }

    public String getTableNameString() {
        return tableNameString;
    }

    public void setTableNameString(String tableNameString) {
        this.tableNameString = tableNameString;
        this.tableName = tableNameString.getBytes();
    }

    public void refreshTableEntry(String familyString, boolean hasTestLoad) throws Exception {
        checkStatus();
        this.ohTable.refreshTableEntry(familyString, hasTestLoad);
    }

    public byte[][] getStartKeys() throws IOException {
        checkStatus();
        return this.ohTable.getStartKeys();
    }

    public byte[][] getEndKeys() throws IOException {
        checkStatus();
        return this.ohTable.getEndKeys();
    }

    public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
        checkStatus();
        return this.ohTable.getStartEndKeys();
    }
}
