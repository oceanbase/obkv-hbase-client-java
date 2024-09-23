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

import com.alipay.oceanbase.hbase.constants.OHConstants;
import com.alipay.oceanbase.hbase.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.hbase.util.KeyDefiner;
import com.alipay.oceanbase.hbase.util.OHTableFactory;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PoolMap;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT;

public class OHTablePool implements Closeable {

    private String originTabelName = null;
    private final PoolMap<String, HTableInterface> tables;
    private final int                              maxSize;
    private final PoolMap.PoolType                 poolType;
    private final Configuration                    config;
    private final HTableInterfaceFactory           tableFactory;

    // A map of table attributes used for the table created by this pool. The map
    // key is composed of Table_Name + SEPARATOR + Attribute_Name, and the value
    // is byte value of attribute.
    private ConcurrentHashMap<String, byte[]>      tableAttributes;

    private ConcurrentHashMap<String, Object>      tableExtendAttributes;

    /**
     * Default Constructor. Default HBaseConfiguration and no limit on pool size.
     */
    public OHTablePool() {
        this(new Configuration(), Integer.MAX_VALUE);
    }

    /**
     * Constructor to set maximum versions and use the specified configuration.
     *
     * @param config  configuration
     * @param maxSize maximum number of references to keep for each table
     */
    public OHTablePool(final Configuration config, final int maxSize) {
        this(config, maxSize, null, null);
    }

    /**
     * Constructor to set maximum versions and use the specified configuration and
     * table factory.
     *
     * @param config       configuration
     * @param maxSize      maximum number of references to keep for each table
     * @param tableFactory table factory
     */
    public OHTablePool(final Configuration config, final int maxSize,
                       final HTableInterfaceFactory tableFactory) {
        this(config, maxSize, tableFactory, PoolMap.PoolType.Reusable);
    }

    /**
     * Constructor to set maximum versions and use the specified configuration and
     * pool type.
     *
     * @param config   configuration
     * @param maxSize  maximum number of references to keep for each table
     * @param poolType pool type which is one of {@link PoolMap.PoolType#Reusable} or
     *                 {@link PoolMap.PoolType#ThreadLocal}
     */
    public OHTablePool(final Configuration config, final int maxSize,
                       final PoolMap.PoolType poolType) {
        this(config, maxSize, null, poolType);
    }

    /**
     * Constructor to set maximum versions and use the specified configuration,
     * table factory and pool type. The HTablePool supports the
     * {@link PoolMap.PoolType#Reusable} and {@link PoolMap.PoolType#ThreadLocal}. If the pool
     * type is null or not one of those two values, then it will default to
     * {@link PoolMap.PoolType#Reusable}.
     *
     * @param config       configuration
     * @param maxSize      maximum number of references to keep for each table
     * @param tableFactory table factory
     * @param poolType     pool type which is one of {@link PoolMap.PoolType#Reusable} or
     *                     {@link PoolMap.PoolType#ThreadLocal}
     */
    public OHTablePool(final Configuration config, final int maxSize,
                       final HTableInterfaceFactory tableFactory, PoolMap.PoolType poolType) {
        this(config, maxSize, null, null, poolType);
    }

    public OHTablePool(final Configuration config, final int maxSize,
        final HTableInterfaceFactory tableFactory,
        final ExecutorService createTableExecutor, PoolMap.PoolType poolType) {
        // Make a new configuration instance so I can safely cleanup when
        // done with the pool.
        this.config = config == null ? new Configuration() : config;
        // Initialize connection when constructing htablepool rather than creating
        // htable
        this.maxSize = maxSize;
        if (tableFactory == null) {
            this.tableFactory = createTableExecutor == null ? new OHTableFactory(this.config, this)
                : new OHTableFactory(this.config, this, createTableExecutor);
        } else {
            this.tableFactory = tableFactory;
        }
        if (poolType == null) {
            this.poolType = PoolMap.PoolType.Reusable;
        } else {
            switch (poolType) {
                case Reusable:
                case ThreadLocal:
                    this.poolType = poolType;
                    break;
                default:
                    this.poolType = PoolMap.PoolType.Reusable;
                    break;
            }
        }
        this.tables = new PoolMap<>(this.poolType, this.maxSize);
    }

    /**
     * Get a reference to the specified table from the pool.
     *
     * @param tableName table name
     * @return a reference to the specified table
     * @throws RuntimeException if there is a problem instantiating the HTable
     */
    public HTableInterface getTable(String tableName) {
        // call the old getTable implementation renamed to findOrCreateTable
        HTableInterface table = findOrCreateTable(tableName);
        // return a proxy table so when user closes the proxy, the actual table
        // will be returned to the pool
        if (table instanceof PooledOHTable) {
            return table;
        } else {
            return new PooledOHTable(table);
        }
    }

    /**
     * Get a reference to the specified table from the pool.
     * <p>
     * <p>
     * Create a new one if one is not available.
     *
     * @param tableName table name
     * @return a reference to the specified table
     * @throws RuntimeException if there is a problem instantiating the HTable
     */
    private HTableInterface findOrCreateTable(String tableName) {
        HTableInterface table = tables.get(tableName);
        if (table == null) {
            table = createHTable(tableName);
        }
        return table;
    }

    /**
     * Get a reference to the specified table from the pool.
     *
     * Create a new one if one is not available.
     *
     * @param tableName table name
     * @return a reference to the specified table
     * @throws RuntimeException if there is a problem instantiating the HTable
     */
    public HTableInterface getTable(byte[] tableName) {
        return getTable(Bytes.toString(tableName));
    }

    /**
     * This method is not needed anymore, clients should call
     * HTableInterface.close() rather than returning the tables to the pool
     *
     * @param table the proxy table user got from pool
     * @throws IOException if failed
     * @deprecated
     */
    public void putTable(HTableInterface table) throws IOException {
        // we need to be sure nobody puts a proxy implementation in the pool
        // but if the client code is not updated
        // and it will continue to call putTable() instead of calling close()
        // then we need to return the wrapped table to the pool instead of the
        // proxy
        // table
        if (table instanceof PooledOHTable) {
            returnTable(((PooledOHTable) table).getWrappedTable());
        } else {
            // normally this should not happen if clients pass back the same
            // table
            // object they got from the pool
            // but if it happens then it's better to reject it
            throw new IllegalArgumentException("not a pooled table: " + table);
        }
    }

    /**
     * Puts the specified HTable back into the pool.
     * <p>
     * <p>
     * If the pool already contains <i>maxSize</i> references to the table, then
     * the table instance gets closed after flushing buffered edits.
     *
     * @param table table
     */
    private void returnTable(HTableInterface table) throws IOException {
        // this is the old putTable method renamed and made private
        String tableName = Bytes.toString(table.getTableName());
        if (tables.size(tableName) >= maxSize) {
            // release table instance since we're not reusing it
            this.tables.remove(tableName, table);
            this.tableFactory.releaseHTableInterface(table);
            return;
        }
        tables.put(tableName, table);
    }

    protected HTableInterface createHTable(String tableName) {
        return this.tableFactory.createHTableInterface(config, Bytes.toBytes(tableName));
    }

    /**
     * Closes all the HTable instances , belonging to the given table, in the
     * table pool.
     *
     * Note: this is a 'shutdown' of the given table pool and different from
     * {@link #putTable(HTableInterface)}, that is used to return the table
     * instance to the pool for future re-use.
     *
     * @param tableName table name
     * @throws IOException if failed
     */
    public void closeTablePool(final String tableName) throws IOException {
        Collection<HTableInterface> tables = this.tables.values(tableName);
        if (tables != null) {
            for (HTableInterface table : tables) {
                this.tableFactory.releaseHTableInterface(table);
            }
        }
        this.tables.remove(tableName);
    }

    /**
     * See {@link #closeTablePool(String)}.
     *
     * @param tableName table name
     * @throws IOException if failed
     */
    public void closeTablePool(final byte[] tableName) throws IOException {
        closeTablePool(Bytes.toString(tableName));
    }

    /**
     * Closes all the HTable instances , belonging to all tables in the table
     * pool.
     * Note: this is a 'shutdown' of all the table pools.
     */
    public void close() throws IOException {
        for (String tableName : tables.keySet()) {
            closeTablePool(tableName);
        }
        this.tables.clear();
        // close resources if instance is OHTableFactory
        if (tableFactory != null && tableFactory instanceof OHTableFactory) {
            ((OHTableFactory) tableFactory).close();
        }
    }

    int getCurrentPoolSize(String tableName) {
        return tables.size(tableName);
    }

    /**
     * See {@link OHConstants#HBASE_OCEANBASE_PARAM_URL}
     *
     * @param tableName table name
     * @param paramUrl  the table root server http url
     */
    public void setParamUrl(final String tableName, String paramUrl) {
        if (originTabelName == null) {
            originTabelName = tableName;
        }
        setTableAttribute(tableName, HBASE_OCEANBASE_PARAM_URL, Bytes.toBytes(paramUrl));
    }

    /**
     * Gets the param url for the specified table in this pool.
     *
     * @param tableName table name
     * @return the table root server http url
     */
    public String getParamUrl(final String tableName) {
        return Bytes.toString(getTableAttribute(tableName, HBASE_OCEANBASE_PARAM_URL));
    }

    /**
     * See {@link OHConstants#HBASE_OCEANBASE_FULL_USER_NAME}
     *
     * @param tableName table name
     * @param fullUserName the table login username
     */
    public void setFullUserName(final String tableName, final String fullUserName) {
        if (originTabelName == null) {
            originTabelName = tableName;
        }
        setTableAttribute(tableName, HBASE_OCEANBASE_FULL_USER_NAME, Bytes.toBytes(fullUserName));
    }

    /**
     * Gets the username for the specified table in this pool.
     *
     * @param tableName table name
     * @return the table login username
     */
    public String getFullUserName(final String tableName) {
        return Bytes.toString(getTableAttribute(tableName, HBASE_OCEANBASE_FULL_USER_NAME));
    }

    /**
     * See {@link OHConstants#HBASE_OCEANBASE_PASSWORD}
     *
     * @param tableName table name
     * @param password  the table login password
     */
    public void setPassword(final String tableName, final String password) {
        if (originTabelName == null) {
            originTabelName = tableName;
        }
        setTableAttribute(tableName, HBASE_OCEANBASE_PASSWORD, Bytes.toBytes(password));
    }

    /**
     * Gets the password for the specified table in this pool.
     *
     * @param tableName table name
     * @return the table login password
     */
    public String getPassword(final String tableName) {
        return Bytes.toString(getTableAttribute(tableName, HBASE_OCEANBASE_PASSWORD));
    }

    /**
     * See {@link OHConstants#HBASE_OCEANBASE_SYS_USER_NAME}
     *
     * @param tableName table name
     * @param sysUserName  the sys username
     */
    public void setSysUserName(final String tableName, final String sysUserName) {
        if (originTabelName == null) {
            originTabelName = tableName;
        }
        setTableAttribute(tableName, HBASE_OCEANBASE_SYS_USER_NAME, Bytes.toBytes(sysUserName));
    }

    /**
     * Gets the sys username for the specified table in this pool.
     *
     * @param tableName table name
     * @return the sys username
     */
    public String getSysUserName(final String tableName) {
        return Bytes.toString(getTableAttribute(tableName, HBASE_OCEANBASE_SYS_PASSWORD));
    }

    /**
     * See {@link OHConstants#HBASE_OCEANBASE_SYS_PASSWORD}
     *
     * @param tableName table name
     * @param sysPassword  the sys user password
     */
    public void setSysPassword(final String tableName, final String sysPassword) {
        if (originTabelName == null) {
            originTabelName = tableName;
        }
        setTableAttribute(tableName, HBASE_OCEANBASE_SYS_PASSWORD, Bytes.toBytes(sysPassword));
    }

    /**
     * Gets the sys user password for the specified table in this pool.
     *
     * @param tableName table name
     * @return the sys user password
     */
    public String getSysPassword(final String tableName) {
        return Bytes.toString(getTableAttribute(tableName, HBASE_OCEANBASE_SYS_PASSWORD));
    }

    /**
     * See {@link #setAutoFlush(String, boolean, boolean)}
     *
     * @param tableName table name
     * @param autoFlush Whether or not to enable 'auto-flush'.
     */
    public void setAutoFlush(final String tableName, boolean autoFlush) {
        setAutoFlush(tableName, autoFlush, autoFlush);
    }

    /**
     * Sets the autoFlush flag for the specified tables in this pool.
     * <p>
     * See {@link OHTable#setAutoFlush(boolean, boolean)}
     *
     * @param tableName         table name
     * @param autoFlush         Whether or not to enable 'auto-flush'.
     * @param clearBufferOnFail Whether to keep Put failures in the writeBuffer
     */
    public void setAutoFlush(final String tableName, boolean autoFlush, boolean clearBufferOnFail) {
        setTableAttribute(tableName, HBASE_HTABLE_POOL_AUTO_FLUSH, Bytes.toBytes(autoFlush));
        setTableAttribute(tableName, HBASE_HTABLE_POOL_CLEAR_BUFFER_ON_FAIL,
            Bytes.toBytes(clearBufferOnFail));
    }

    /**
     * Gets the autoFlush flag for the specified tables in this pool.
     *
     * @param tableName table name
     * @return true if enable 'auto-flush' for specified table
     */
    public boolean getAutoFlush(String tableName) {
        byte[] attr = getTableAttribute(tableName, HBASE_HTABLE_POOL_AUTO_FLUSH);
        return attr == null || Bytes.toBoolean(attr);
    }

    /**
     * Gets the flag of ClearBufferOnFail for the specified tables in this pool.
     *
     * @param tableName table name
     * @return true if keep Put failures in the writeBuffer
     */
    public boolean getClearBufferOnFail(String tableName) {
        byte[] attr = getTableAttribute(tableName, HBASE_HTABLE_POOL_CLEAR_BUFFER_ON_FAIL);
        return attr == null || Bytes.toBoolean(attr);
    }

    /**
     * Sets the size of the buffer in bytes for the specified tables in this pool.
     * <p>
     * See {@link HTable#setWriteBufferSize(long)}
     *
     * @param tableName table name
     * @param writeBufferSize The new write buffer size, in bytes.
     * @throws IOException if a remote or network exception occurs.
     */
    public void setWriteBufferSize(final String tableName, long writeBufferSize) throws IOException {
        setTableAttribute(tableName, HBASE_HTABLE_POOL_WRITE_BUFFER_SIZE,
            Bytes.toBytes(writeBufferSize));
    }

    /**
     * Get the maximum size in bytes of the write buffer for the specified tables
     * in this pool.
     *
     * @param tableName table name
     * @return The size of the write buffer in bytes.
     */
    public long getWriteBufferSize(String tableName) {
        byte[] attr = getTableAttribute(tableName, HBASE_HTABLE_POOL_WRITE_BUFFER_SIZE);
        return attr == null ? this.config.getLong(WRITE_BUFFER_SIZE_KEY, 2097152) : Bytes
            .toLong(attr);
    }

    public String getOriginTableName() {
        return this.originTabelName;
    }

    /**
     * Sets the operation timeout for the specified tables in this pool.
     *
     * @param tableName table name
     * @param operationTimeout timeout
     */
    public void setOperationTimeout(final String tableName, int operationTimeout) {
        setTableAttribute(tableName, HBASE_HTABLE_POOL_OPERATION_TIMEOUT,
            Bytes.toBytes(operationTimeout));
    }

    public void refreshTableEntry(final String tableName, final String family, boolean hasTestLoad)
                                                                                                   throws Exception {
        ((OHTable) ((PooledOHTable) getTable(tableName)).getTable()).refreshTableEntry(family,
            hasTestLoad);
    }

    /**
     * Gets the operation timeout for the specified tables in this pool.
     *
     * @param tableName table name
     * @return the operation timeout
     */
    public int getOperationTimeout(String tableName) {
        byte[] attr = getTableAttribute(tableName, HBASE_HTABLE_POOL_OPERATION_TIMEOUT);
        if (attr == null) {
            return DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT;
        } else {
            return Bytes.toInt(attr);
        }
    }

    /**
     * Sets the ODP address for the specified tables in this pool.
     *
     * @param tableName table name
     * @param odpAddr ODP address
     */
    public void setOdpAddr(final String tableName, String odpAddr) {
        if (originTabelName == null) {
            originTabelName = tableName;
        }
        setTableAttribute(tableName, HBASE_OCEANBASE_ODP_ADDR, Bytes.toBytes(odpAddr));
    }

    /**
     * Gets the ODP address for the specified tables in this pool.
     *
     * @param tableName table name
     * @return the ODP address
     */
    public String getOdpAddr(String tableName) {
        byte[] attr = getTableAttribute(tableName, HBASE_OCEANBASE_ODP_ADDR);
        if (attr == null) {
            return "";
        } else {
            return Bytes.toString(attr);
        }
    }

    /**
     * Sets the ODP port for the specified tables in this pool.
     *
     * @param tableName table name
     * @param odpPort ODP port
     */
    public void setOdpPort(final String tableName, int odpPort) {
        if (originTabelName == null) {
            originTabelName = tableName;
        }
        setTableAttribute(tableName, HBASE_OCEANBASE_ODP_PORT, Bytes.toBytes(odpPort));
    }

    /**
     * Gets the ODP port for the specified tables in this pool.
     * @param tableName table name
     * @return the ODP port
     */
    public int getOdpPort(String tableName) {
        byte[] attr = getTableAttribute(tableName, HBASE_OCEANBASE_ODP_PORT);
        return attr == null ? this.config.getInt(HBASE_OCEANBASE_ODP_PORT, -1) : Bytes.toInt(attr);
    }

    /**
     * Sets the ODP mode for the specified tables in this pool.
     * @param tableName table name
     * @param odpMode ODP mode
     */
    public void setOdpMode(final String tableName, boolean odpMode) {
        if (originTabelName == null) {
            originTabelName = tableName;
        }
        setTableAttribute(tableName, HBASE_OCEANBASE_ODP_MODE, Bytes.toBytes(odpMode));
    }

    /**
     * Gets the ODP mode for the specified tables in this pool.
     * @param tableName table name
     * @return the ODP mode
     */
    public boolean getOdpMode(String tableName) {
        byte[] attr = getTableAttribute(tableName, HBASE_OCEANBASE_ODP_MODE);
        return attr != null && Bytes.toBoolean(attr);
    }

    /**
     * Sets the ODP database name for the specified tables in this pool.
     * @param tableName table name
     * @param database ODP database name
     */
    public void setDatabase(final String tableName, String database) {
        if (originTabelName == null) {
            originTabelName = tableName;
        }
        setTableAttribute(tableName, HBASE_OCEANBASE_DATABASE, Bytes.toBytes(database));
    }

    /**
     * Gets the ODP database name for the specified tables in this pool.
     * @param tableName table name
     * @return the ODP database name
     */
    public String getDatabase(String tableName) {
        byte[] attr = getTableAttribute(tableName, HBASE_OCEANBASE_DATABASE);
        if (attr == null) {
            return "";
        } else {
            return Bytes.toString(attr);
        }
    }

    private void setTableAttribute(String tableName, String attributeName, byte[] value) {
        if (tableAttributes == null && value == null) {
            return;
        }
        if (tableAttributes == null) {
            tableAttributes = new ConcurrentHashMap<String, byte[]>();
        }
        String name = KeyDefiner.genPooledOHTableAttributeName(tableName, attributeName);
        if (value == null) {
            tableAttributes.remove(name);
            if (tableAttributes.isEmpty()) {
                this.tableAttributes = null;
            }
        } else {
            tableAttributes.put(name, value);
        }
    }

    public byte[] getTableAttribute(String tableName, String attributeName) {
        if (tableAttributes == null) {
            return null;
        }
        String name = KeyDefiner.genPooledOHTableAttributeName(tableName, attributeName);
        return tableAttributes.get(name);
    }

    private void setTableExtendAttribute(String tableName, String attributeName, Object value) {
        if (tableExtendAttributes == null && value == null) {
            return;
        }
        if (tableExtendAttributes == null) {
            tableExtendAttributes = new ConcurrentHashMap<String, Object>();
        }
        String name = KeyDefiner.genPooledOHTableAttributeName(tableName, attributeName);
        if (value == null) {
            tableExtendAttributes.remove(name);
            if (tableExtendAttributes.isEmpty()) {
                this.tableExtendAttributes = null;
            }
        } else {
            tableExtendAttributes.put(name, value);
        }
    }

    public Object getTableExtendAttribute(String tableName, String attributeName) {
        if (tableExtendAttributes == null) {
            return null;
        }
        String name = KeyDefiner.genPooledOHTableAttributeName(tableName, attributeName);
        return tableExtendAttributes.get(name);
    }

    public void setRuntimeBatchExecutor(String tableName, ExecutorService runtimeBatchExecutor) {
        if (originTabelName == null) {
            originTabelName = tableName;
        }
        setTableExtendAttribute(tableName, HBASE_OCEANBASE_BATCH_EXECUTOR, runtimeBatchExecutor);
    }

    public ExecutorService getRuntimeBatchExecutor(String tableName) {
        return (ExecutorService) getTableExtendAttribute(tableName, HBASE_OCEANBASE_BATCH_EXECUTOR);
    }

    /**
     * A proxy class that implements HTableInterface.close method to return the
     * wrapped table back to the table pool
     */
    public class PooledOHTable implements HTableInterface {

        private HTableInterface table; // actual table implementation

        public PooledOHTable(HTableInterface table) {
            this.table = table;
        }

        @Override
        public byte[] getTableName() {
            return table.getTableName();
        }

        @Override
        public TableName getName() {
            throw new FeatureNotSupportedException("not supported yet'");
        }

        @Override
        public Configuration getConfiguration() {
            return table.getConfiguration();
        }

        @Override
        public HTableDescriptor getTableDescriptor() throws IOException {
            return table.getTableDescriptor();
        }

        @Override
        public boolean exists(Get get) throws IOException {
            return table.exists(get);
        }

        @Override
        public boolean[] existsAll(List<Get> list) throws IOException {
            return table.existsAll(list);
        }

        @Override
        public Boolean[] exists(List<Get> gets) throws IOException {
            return table.exists(gets);
        }

        @Override
        public void batch(List<? extends Row> actions, Object[] results) throws IOException,
                                                                        InterruptedException {
            table.batch(actions, results);
        }

        @Override
        public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
            return table.batch(actions);
        }

        @Override
        public <R> void batchCallback(List<? extends Row> actions, Object[] results,
                                      Batch.Callback<R> callback) throws IOException,
                                                                 InterruptedException {
            throw new FeatureNotSupportedException("not supported yet'");
        }

        @Override
        public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback)
                                                                                                  throws IOException,
                                                                                                  InterruptedException {
            throw new FeatureNotSupportedException("not supported yet'");
        }

        @Override
        public Result get(Get get) throws IOException {
            return table.get(get);
        }

        @Override
        public Result[] get(List<Get> gets) throws IOException {
            return table.get(gets);
        }

        @Override
        @SuppressWarnings("deprecation")
        public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
            return table.getRowOrBefore(row, family);
        }

        @Override
        public ResultScanner getScanner(Scan scan) throws IOException {
            return table.getScanner(scan);
        }

        @Override
        public ResultScanner getScanner(byte[] family) throws IOException {
            return table.getScanner(family);
        }

        @Override
        public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
            return table.getScanner(family, qualifier);
        }

        @Override
        public void put(Put put) throws IOException {
            table.put(put);
        }

        @Override
        public void put(List<Put> puts) throws IOException {
            table.put(puts);
        }

        @Override
        public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value,
                                   Put put) throws IOException {
            return table.checkAndPut(row, family, qualifier, value, put);
        }

        @Override
        public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
                                   CompareFilter.CompareOp compareOp, byte[] value, Put put)
                                                                                            throws IOException {
            return table.checkAndPut(row, family, qualifier, compareOp, value, put);
        }

        @Override
        public void delete(Delete delete) throws IOException {
            table.delete(delete);
        }

        @Override
        public void delete(List<Delete> deletes) throws IOException {
            table.delete(deletes);
        }

        @Override
        public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
                                      Delete delete) throws IOException {
            return table.checkAndDelete(row, family, qualifier, value, delete);
        }

        @Override
        public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
                                      CompareFilter.CompareOp compareOp, byte[] value, Delete delete)
                                                                                                     throws IOException {
            return table.checkAndDelete(row, family, qualifier, compareOp, value, delete);
        }

        @Override
        public Result increment(Increment increment) throws IOException {
            return table.increment(increment);
        }

        @Override
        public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
                                                                                                  throws IOException {
            return table.incrementColumnValue(row, family, qualifier, amount);
        }

        @Override
        public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
                                         Durability durability) throws IOException {
            return table.incrementColumnValue(row, family, qualifier, amount, durability);
        }

        @Override
        public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
                                         boolean writeToWAL) throws IOException {
            return table.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
        }

        @Override
        public boolean isAutoFlush() {
            return table.isAutoFlush();
        }

        @Override
        public void flushCommits() throws IOException {
            table.flushCommits();
        }

        /**
         * Returns the actual table back to the pool
         *
         * @throws IOException if failed
         */
        public void close() throws IOException {
            returnTable(table);
        }

        @Override
        public CoprocessorRpcChannel coprocessorService(byte[] row) {
            return table.coprocessorService(row);
        }

        @Override
        public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service,
                                                                        byte[] startKey,
                                                                        byte[] endKey,
                                                                        Batch.Call<T, R> callable)
                                                                                                  throws ServiceException,
                                                                                                  Throwable {
            return table.coprocessorService(service, startKey, endKey, callable);
        }

        @Override
        public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
                                                              byte[] endKey,
                                                              Batch.Call<T, R> callable,
                                                              Batch.Callback<R> callback)
                                                                                         throws ServiceException,
                                                                                         Throwable {
            table.coprocessorService(service, startKey, endKey, callable, callback);
        }

        @Override
        public String toString() {
            return "PooledOHTable{" + ", table=" + table + '}';
        }

        /**
         * Expose the wrapped HTable to tests in the same package
         *
         * @return wrapped htable
         */
        HTableInterface getWrappedTable() {
            return table;
        }

        @Override
        public void mutateRow(RowMutations rm) throws IOException {
            table.mutateRow(rm);
        }

        @Override
        public Result append(Append append) throws IOException {
            return table.append(append);
        }

        @Override
        public void setAutoFlush(boolean autoFlush) {
            table.setAutoFlush(autoFlush);
        }

        @Override
        public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
            table.setAutoFlush(autoFlush, clearBufferOnFail);
        }

        @Override
        public void setAutoFlushTo(boolean autoFlush) {
            table.setAutoFlushTo(autoFlush);
        }

        @Override
        public long getWriteBufferSize() {
            return table.getWriteBufferSize();
        }

        @Override
        public void setWriteBufferSize(long writeBufferSize) throws IOException {
            table.setWriteBufferSize(writeBufferSize);
        }

        @Override
        public <R extends Message> Map<byte[], R> batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor,
                                                                          Message request,
                                                                          byte[] startKey,
                                                                          byte[] endKey,
                                                                          R responsePrototype)
                                                                                              throws ServiceException,
                                                                                              Throwable {
            return table.batchCoprocessorService(methodDescriptor, request, startKey, endKey,
                responsePrototype);
        }

        @Override
        public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor,
                                                                Message request, byte[] startKey,
                                                                byte[] endKey, R responsePrototype,
                                                                Batch.Callback<R> callback)
                                                                                           throws ServiceException,
                                                                                           Throwable {
            table.batchCoprocessorService(methodDescriptor, request, startKey, endKey,
                responsePrototype, callback);
        }

        @Override
        public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
                                      CompareFilter.CompareOp compareOp, byte[] value,
                                      RowMutations mutations) throws IOException {
            return table.checkAndMutate(row, family, qualifier, compareOp, value, mutations);
        }

        @Override
        public void setOperationTimeout(int i) {
            table.setOperationTimeout(i);
        }

        @Override
        public int getOperationTimeout() {
            return table.getOperationTimeout();
        }

        @Override
        public void setRpcTimeout(int i) {
            table.setRpcTimeout(i);
        }

        @Override
        public int getRpcTimeout() {
            return table.getRpcTimeout();
        }

        public HTableInterface getTable() {
            return table;
        }

        public byte[][] getStartKeys() throws IOException {
            return ((OHTable) this.table).getStartKeys();
        }

        public byte[][] getEndKeys() throws IOException {
            return ((OHTable) this.table).getEndKeys();
        }

        public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
            return ((OHTable) this.table).getStartEndKeys();
        }
    }
}
