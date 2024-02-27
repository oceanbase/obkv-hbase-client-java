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
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

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
    public byte[] getTableName() {
        return tableName;
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
        this.ohTable.refreshTableEntry(familyString, hasTestLoad);
    }

    public byte[][] getStartKeys() throws IOException {
        return this.ohTable.getStartKeys();
    }

    public byte[][] getEndKeys() throws IOException {
        return this.ohTable.getEndKeys();
    }

    public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
        return this.ohTable.getStartEndKeys();
    }
    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(Descriptors.MethodDescriptor var1,
                                                                      Message var2, byte[] var3,
                                                                      byte[] var4, R var5)
                                                                                          throws ServiceException,
                                                                                          Throwable {
        throw new IllegalArgumentException("Not implemented");
    }

    @Override
    public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor var1,
                                                            Message var2, byte[] var3, byte[] var4,
                                                            R var5, Batch.Callback<R> var6)
                                                                                           throws ServiceException,
                                                                                           Throwable {
        throw new IllegalArgumentException("Not implemented");
    }

    @Override
    public boolean checkAndMutate(byte[] var1, byte[] var2, byte[] var3,
                                  CompareFilter.CompareOp var4, byte[] var5, RowMutations var6)
                                                                                               throws IOException {
        throw new IllegalArgumentException("Not implemented");
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] var1) {
        return null;
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> var1, byte[] var2,
                                                                    byte[] var3,
                                                                    Batch.Call<T, R> var4)
                                                                                          throws ServiceException,
                                                                                          Throwable {
        throw new IllegalArgumentException("Not implemented");
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> var1, byte[] var2, byte[] var3,
                                                          Batch.Call<T, R> var4,
                                                          Batch.Callback<R> var5)
                                                                                 throws ServiceException,
                                                                                 Throwable {
        throw new IllegalArgumentException("Not implemented");
    }

    @Override
    public long incrementColumnValue(byte[] var1, byte[] var2, byte[] var3, long var4,
                                     Durability var6) throws IOException {
        throw new IllegalArgumentException("Not implemented");
    }

    @Override
    public boolean checkAndDelete(byte[] var1, byte[] var2, byte[] var3,
                                  CompareFilter.CompareOp var4, byte[] var5, Delete var6)
                                                                                         throws IOException {
        throw new IllegalArgumentException("Not implemented");
    }

    @Override
    public boolean checkAndPut(byte[] var1, byte[] var2, byte[] var3, CompareFilter.CompareOp var4,
                               byte[] var5, Put var6) throws IOException {
        throw new IllegalArgumentException("Not implemented");
    }

    @Override
    public <R> void batchCallback(List<? extends Row> var1, Object[] var2, Batch.Callback<R> var3)
                                                                                                  throws IOException,
                                                                                                  InterruptedException {
        throw new IllegalArgumentException("Not implemented");
    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> var1, Batch.Callback<R> var2)
                                                                                       throws IOException,
                                                                                       InterruptedException {
        throw new IllegalArgumentException("Not implemented");
    }

    @Override
    public TableName getName() {
        return null;
    }

    @Override
    public boolean[] existsAll(List<Get> var1) throws IOException {
        throw new IllegalArgumentException("Not implemented");
    }

    @Override
    public Boolean[] exists(List<Get> var1) throws IOException {
        throw new IllegalArgumentException("Not implemented");
    }

    @Override
    public void setAutoFlushTo(boolean autoFlush) {
        ohTable.setAutoFlush(autoFlush);
    }
}
