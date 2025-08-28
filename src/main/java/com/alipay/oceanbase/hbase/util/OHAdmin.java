package com.alipay.oceanbase.hbase.util;

import com.alipay.oceanbase.hbase.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.bolt.transport.TransportCodes;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.exception.ObTableTransportException;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import static com.alipay.oceanbase.rpc.protocol.payload.ResultCodes.*;

public class OHAdmin implements Admin {
    private boolean                 aborted = false;
    private final OHConnectionImpl  connection;
    private final Configuration     conf;

    @FunctionalInterface
    private interface ExceptionHandler {
        void handle(int errorCode, TableName tableName) throws IOException;
    }

    private Throwable getRootCause(Throwable e) {
        Throwable cause = e.getCause();
        while(cause != null && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    private void handleTimeoutException(Exception e) throws TimeoutIOException {
        if (e.getCause() instanceof ObTableTransportException
                && ((ObTableTransportException) e.getCause()).getErrorCode() == TransportCodes.BOLT_TIMEOUT) {
            throw new TimeoutIOException(e.getCause());
        }
    }

    private void handleObTableException(Exception e, TableName tableName, ExceptionHandler exceptionHandler) throws IOException {
        if (e instanceof IOException) {
            handleTimeoutException(e);
        }

        Throwable cause = getRootCause(e);

        if (cause instanceof ObTableException) {
            int errCode = ((ObTableException) cause).getErrorCode();
            try {
                exceptionHandler.handle(errCode, tableName);
            } catch (RuntimeException re) {
                throw re;
            }
        }

        if (e instanceof IOException) {
            throw (IOException) e;
        } else {
            throw new IOException(e);
        }
    }
    OHAdmin(OHConnectionImpl connection) {
        this.connection = connection;
        this.conf = connection.getConfiguration();
    }

    @Override
    public int getOperationTimeout() {
        return connection.getOHConnectionConfiguration().getClientOperationTimeout();
    }

    @Override
    public void abort(String msg, Throwable t) {
        // do nothing, just throw the message and exception
        this.aborted = true;
        throw new RuntimeException(msg, t);
    }

    @Override
    public boolean isAborted() {
        return this.aborted;
    }

    @Override
    public Connection getConnection() {
        return this.connection;
    }

    @Override
    public boolean tableExists(TableName tableName) throws IOException {
        try {
            OHConnectionConfiguration connectionConf = new OHConnectionConfiguration(conf);
            ObTableClient tableClient = ObTableClientManager.getOrCreateObTableClientByTableName(tableName, connectionConf);
            OHTableExistsExecutor executor = new OHTableExistsExecutor(tableClient);
            return executor.tableExists(tableName.getNameAsString());
        } catch (Exception e) {
            // try to get the original cause
            Throwable cause = getRootCause(e);
            if (cause instanceof ObTableException) {
                int errCode = ((ObTableException) cause).getErrorCode();
                // if the original cause is database_not_exist, means namespace in tableName does not exist
                // for HBase, namespace not exist will not throw exceptions but will return false
                if (errCode == ResultCodes.OB_ERR_BAD_DATABASE.errorCode) {
                    return false;
                }
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e);
            }
        }
    }

    @Override
    public HTableDescriptor[] listTables() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public HTableDescriptor[] listTables(String s) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public HTableDescriptor[] listTables(Pattern pattern, boolean b) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public HTableDescriptor[] listTables(String s, boolean b) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public TableName[] listTableNames() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public TableName[] listTableNames(Pattern pattern) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public TableName[] listTableNames(String s) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public TableName[] listTableNames(Pattern pattern, boolean b) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public TableName[] listTableNames(String s, boolean b) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public HTableDescriptor getTableDescriptor(TableName tableName) throws TableNotFoundException, IOException {
        OHConnectionConfiguration connectionConf = new OHConnectionConfiguration(conf);
        ObTableClient tableClient = ObTableClientManager.getOrCreateObTableClientByTableName(tableName, connectionConf);
        OHTableDescriptorExecutor executor = new OHTableDescriptorExecutor(tableName.getNameAsString(), tableClient);
        try {
            return executor.getTableDescriptor();
        } catch (IOException e) {
            handleObTableException(e, tableName, (errCode, argTableName) -> {
                if (errCode == OB_KV_HBASE_TABLE_NOT_EXISTS.errorCode) {
                    throw new TableNotFoundException(argTableName);
                } else if (errCode == OB_KV_HBASE_NAMESPACE_NOT_FOUND.errorCode) {
                    throw new NamespaceNotFoundException(argTableName.getNamespaceAsString());
                }
            });
            throw e; // should never reach
        }
    }

    @Override
    public void createTable(HTableDescriptor tableDescriptor) throws IOException {
        OHConnectionConfiguration connectionConf = new OHConnectionConfiguration(conf);
        ObTableClient tableClient = ObTableClientManager.getOrCreateObTableClientByTableName(tableDescriptor.getTableName(), connectionConf);
        OHCreateTableExecutor executor = new OHCreateTableExecutor(tableClient);
        try {
            executor.createTable(tableDescriptor, null);
        } catch (IOException e) {
            handleObTableException(e, tableDescriptor.getTableName(), (errCode, tableName) -> {
                if (errCode == OB_KV_HBASE_TABLE_EXISTS.errorCode) {
                    throw new TableExistsException(tableName.getNameAsString());
                } else if (errCode == OB_KV_HBASE_NAMESPACE_NOT_FOUND.errorCode) {
                    throw new NamespaceNotFoundException(tableName.getNameAsString());
                }
            });
        }
    }

    @Override
    public void createTable(HTableDescriptor tableDescriptor, byte[] bytes, byte[] bytes1, int i) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void createTable(HTableDescriptor hTableDescriptor, byte[][] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void createTableAsync(HTableDescriptor hTableDescriptor, byte[][] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void deleteTable(TableName tableName) throws IOException {
        OHConnectionConfiguration connectionConf = new OHConnectionConfiguration(conf);
        ObTableClient tableClient = ObTableClientManager.getOrCreateObTableClientByTableName(tableName, connectionConf);
        OHDeleteTableExecutor executor = new OHDeleteTableExecutor(tableClient);
        try {
            executor.deleteTable(tableName.getNameAsString());
        } catch (IOException e) {
            handleObTableException(e, tableName, (errCode, argTableName) -> {
                if (errCode == OB_KV_HBASE_TABLE_NOT_EXISTS.errorCode) {
                    throw new TableNotFoundException(argTableName);
                } else if (errCode == OB_KV_HBASE_NAMESPACE_NOT_FOUND.errorCode) {
                    throw new NamespaceNotFoundException(argTableName.getNamespaceAsString());
                } else if (errCode == OB_KV_TABLE_NOT_DISABLED.errorCode) {
                    throw new TableNotDisabledException(argTableName);
                }
            });
        }
    }

    @Override
    public HTableDescriptor[] deleteTables(String s) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void truncateTable(TableName tableName, boolean b) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void enableTable(TableName tableName) throws IOException {
        OHConnectionConfiguration connectionConf = new OHConnectionConfiguration(conf);
        ObTableClient tableClient = ObTableClientManager.getOrCreateObTableClientByTableName(tableName, connectionConf);
        OHTableAccessControlExecutor executor = new OHTableAccessControlExecutor(tableClient, ObTableRpcMetaType.HTABLE_ENABLE_TABLE);
        try {
            executor.enableTable(tableName.getNameAsString());
        } catch (IOException e) {
            handleObTableException(e, tableName, (errCode, argTableName) -> {
                if (errCode == OB_KV_HBASE_TABLE_NOT_EXISTS.errorCode) {
                    throw new TableNotFoundException(argTableName);
                } else if (errCode == OB_KV_HBASE_NAMESPACE_NOT_FOUND.errorCode) {
                    throw new NamespaceNotFoundException(argTableName.getNamespaceAsString());
                }
            });
        }
    }

    @Override
    public void enableTableAsync(TableName tableName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public HTableDescriptor[] enableTables(String s) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void disableTableAsync(TableName tableName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void disableTable(TableName tableName) throws IOException {
        OHConnectionConfiguration connectionConf = new OHConnectionConfiguration(conf);
        ObTableClient tableClient = ObTableClientManager.getOrCreateObTableClientByTableName(tableName, connectionConf);
        OHTableAccessControlExecutor executor = new OHTableAccessControlExecutor(tableClient, ObTableRpcMetaType.HTABLE_DISABLE_TABLE);
        try {
            executor.disableTable(tableName.getNameAsString());
        } catch (IOException e) {
            handleObTableException(e, tableName, (errCode, argTableName) -> {
                if (errCode == OB_KV_HBASE_TABLE_NOT_EXISTS.errorCode) {
                    throw new TableNotFoundException(argTableName);
                } else if (errCode == OB_KV_HBASE_NAMESPACE_NOT_FOUND.errorCode) {
                    throw new NamespaceNotFoundException(argTableName.getNamespaceAsString());
                }
            });
        }
    }

    @Override
    public HTableDescriptor[] disableTables(String s) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean isTableEnabled(TableName tableName) throws IOException {
        return !isDisabled(tableName);
    }

    @Override
    public boolean isTableDisabled(TableName tableName) throws IOException {
        return isDisabled(tableName);
    }

    private boolean isDisabled(TableName tableName) throws IOException {
        OHConnectionConfiguration connectionConf = new OHConnectionConfiguration(conf);
        ObTableClient tableClient = ObTableClientManager.getOrCreateObTableClientByTableName(tableName, connectionConf);
        OHTableDescriptorExecutor tableDescriptor = new OHTableDescriptorExecutor(tableName.getNameAsString(), tableClient);
        return tableDescriptor.isDisable();
    }

    @Override
    public boolean isTableAvailable(TableName tableName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean isTableAvailable(TableName tableName, byte[][] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public Pair<Integer, Integer> getAlterStatus(TableName tableName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public Pair<Integer, Integer> getAlterStatus(byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void addColumn(TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void deleteColumn(TableName tableName, byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void modifyColumn(TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void closeRegion(String s, String s1) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void closeRegion(byte[] bytes, String s) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean closeRegionWithEncodedRegionName(String s, String s1) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void closeRegion(ServerName serverName, HRegionInfo hRegionInfo) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public List<HRegionInfo> getOnlineRegions(ServerName serverName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void flush(TableName tableName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void flushRegion(byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void compact(TableName tableName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void compactRegion(byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void compact(TableName tableName, byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void compactRegion(byte[] bytes, byte[] bytes1) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void majorCompact(TableName tableName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void majorCompactRegion(byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void majorCompact(TableName tableName, byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void majorCompactRegion(byte[] bytes, byte[] bytes1) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void compactRegionServer(ServerName serverName, boolean b) throws IOException, InterruptedException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void move(byte[] bytes, byte[] bytes1) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void assign(byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void unassign(byte[] bytes, boolean b) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void offline(byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean setBalancerRunning(boolean b, boolean b1) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean balancer() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean balancer(boolean b) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean isBalancerEnabled() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean normalize() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean isNormalizerEnabled() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean setNormalizerRunning(boolean b) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean enableCatalogJanitor(boolean b) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public int runCatalogScan() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean isCatalogJanitorEnabled() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void mergeRegions(byte[] bytes, byte[] bytes1, boolean b) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void split(TableName tableName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void splitRegion(byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void split(TableName tableName, byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void splitRegion(byte[] bytes, byte[] bytes1) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void modifyTable(TableName tableName, HTableDescriptor hTableDescriptor) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void shutdown() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void stopMaster() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void stopRegionServer(String s) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public ClusterStatus getClusterStatus() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public Configuration getConfiguration() {
        return connection.getConfiguration();
    }

    @Override
    public void createNamespace(NamespaceDescriptor namespaceDescriptor) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void modifyNamespace(NamespaceDescriptor namespaceDescriptor) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void deleteNamespace(String s) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public NamespaceDescriptor getNamespaceDescriptor(String s) throws NamespaceNotFoundException, IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public HTableDescriptor[] listTableDescriptorsByNamespace(String s) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public TableName[] listTableNamesByNamespace(String s) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public List<HRegionInfo> getTableRegions(TableName tableName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public synchronized void close() throws IOException {

    }

    @Override
    public HTableDescriptor[] getTableDescriptorsByTableName(List<TableName> tableNames) throws IOException {
        OHConnectionConfiguration connectionConf = new OHConnectionConfiguration(conf);
        List<HTableDescriptor> tableDescriptors = new ArrayList<>();
        for (TableName tableName : tableNames) {
            ObTableClient tableClient = ObTableClientManager.getOrCreateObTableClientByTableName(tableName, connectionConf);
            OHTableDescriptorExecutor executor = new OHTableDescriptorExecutor(tableName.getNameAsString(), tableClient);
            try {
                tableDescriptors.add(executor.getTableDescriptor());
            } catch (IOException e) {
                if (e.getCause() instanceof ObTableTransportException
                        && ((ObTableTransportException) e.getCause()).getErrorCode() == TransportCodes.BOLT_TIMEOUT) {
                    throw new TimeoutIOException(e.getCause());
                } else if (e.getCause().getMessage().contains("OB_TABLEGROUP_NOT_EXIST")) {
                    throw new TableNotFoundException(tableName);
                } else {
                    throw e;
                }
            }
        }
        return tableDescriptors.toArray(new HTableDescriptor[0]);
    }

    @Override
    public HTableDescriptor[] getTableDescriptors(List<String> tableNames) throws IOException {
        List<TableName> tableNameList = new ArrayList<>();
        for (String tableName : tableNames) {
            tableNameList.add(TableName.valueOf(tableName));
        }
        return getTableDescriptorsByTableName(tableNameList);
    }

    @Override
    public boolean abortProcedure(long l, boolean b) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public ProcedureInfo[] listProcedures() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public Future<Boolean> abortProcedureAsync(long l, boolean b) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void rollWALWriter(ServerName serverName) throws IOException, FailedLogCloseException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public String[] getMasterCoprocessors() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionState(TableName tableName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionStateForRegion(byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public long getLastMajorCompactionTimestamp(TableName tableName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public long getLastMajorCompactionTimestampForRegion(byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void snapshot(String s, TableName tableName) throws IOException, SnapshotCreationException, IllegalArgumentException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void snapshot(byte[] bytes, TableName tableName) throws IOException, SnapshotCreationException, IllegalArgumentException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void snapshot(String s, TableName tableName, HBaseProtos.SnapshotDescription.Type type) throws IOException, SnapshotCreationException, IllegalArgumentException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void snapshot(HBaseProtos.SnapshotDescription snapshotDescription) throws IOException, SnapshotCreationException, IllegalArgumentException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public MasterProtos.SnapshotResponse takeSnapshotAsync(HBaseProtos.SnapshotDescription snapshotDescription) throws IOException, SnapshotCreationException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean isSnapshotFinished(HBaseProtos.SnapshotDescription snapshotDescription) throws IOException, HBaseSnapshotException, UnknownSnapshotException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void restoreSnapshot(byte[] bytes) throws IOException, RestoreSnapshotException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void restoreSnapshot(String s) throws IOException, RestoreSnapshotException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void restoreSnapshot(byte[] bytes, boolean b) throws IOException, RestoreSnapshotException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void restoreSnapshot(String s, boolean b) throws IOException, RestoreSnapshotException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void cloneSnapshot(byte[] bytes, TableName tableName) throws IOException, TableExistsException, RestoreSnapshotException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void cloneSnapshot(String s, TableName tableName) throws IOException, TableExistsException, RestoreSnapshotException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void execProcedure(String s, String s1, Map<String, String> map) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public byte[] execProcedureWithRet(String s, String s1, Map<String, String> map) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean isProcedureFinished(String s, String s1, Map<String, String> map) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public List<HBaseProtos.SnapshotDescription> listSnapshots() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public List<HBaseProtos.SnapshotDescription> listSnapshots(String s) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public List<HBaseProtos.SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public List<HBaseProtos.SnapshotDescription> listTableSnapshots(String s, String s1) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public List<HBaseProtos.SnapshotDescription> listTableSnapshots(Pattern pattern, Pattern pattern1) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void deleteSnapshot(byte[] bytes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void deleteSnapshot(String s) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void deleteSnapshots(String s) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void deleteSnapshots(Pattern pattern) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void deleteTableSnapshots(String s, String s1) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void deleteTableSnapshots(Pattern pattern, Pattern pattern1) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void setQuota(QuotaSettings quotaSettings) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public QuotaRetriever getQuotaRetriever(QuotaFilter quotaFilter) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public CoprocessorRpcChannel coprocessorService() {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(ServerName serverName) {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void updateConfiguration(ServerName serverName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public void updateConfiguration() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public int getMasterInfoPort() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public List<SecurityCapability> getSecurityCapabilities() throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean[] setSplitOrMergeEnabled(boolean b, boolean b1, MasterSwitchType... masterSwitchTypes) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public boolean isSplitOrMergeEnabled(MasterSwitchType masterSwitchType) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }
}
