package com.alipay.oceanbase.hbase.util;

import com.alipay.oceanbase.hbase.OHTable;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.bolt.transport.TransportCodes;
import com.alipay.oceanbase.rpc.exception.ObTableTransportException;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

public class OHAdmin implements Admin {
    private final OHConnectionImpl connection;
    private final Configuration conf;
    OHAdmin(OHConnectionImpl connection) {
        this.connection = connection;
        this.conf = connection.getConfiguration();
    }

    @Override
    public int getOperationTimeout() {
        return 0;
    }

    @Override
    public void abort(String s, Throwable throwable) {

    }

    @Override
    public boolean isAborted() {
        return false;
    }

    @Override
    public Connection getConnection() {
        return null;
    }

    @Override
    public boolean tableExists(TableName tableName) throws IOException {
        OHConnectionConfiguration connectionConf = new OHConnectionConfiguration(conf);
        ObTableClient tableClient = ObTableClientManager.getOrCreateObTableClientByTableName(tableName, connectionConf);
        OHTableExistsExecutor executor = new OHTableExistsExecutor(tableClient);
        return executor.tableExists(tableName.getNameAsString());
    }

    @Override
    public HTableDescriptor[] listTables() throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public HTableDescriptor[] listTables(String s) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public HTableDescriptor[] listTables(Pattern pattern, boolean b) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public HTableDescriptor[] listTables(String s, boolean b) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public TableName[] listTableNames() throws IOException {
        return new TableName[0];
    }

    @Override
    public TableName[] listTableNames(Pattern pattern) throws IOException {
        return new TableName[0];
    }

    @Override
    public TableName[] listTableNames(String s) throws IOException {
        return new TableName[0];
    }

    @Override
    public TableName[] listTableNames(Pattern pattern, boolean b) throws IOException {
        return new TableName[0];
    }

    @Override
    public TableName[] listTableNames(String s, boolean b) throws IOException {
        return new TableName[0];
    }

    @Override
    public HTableDescriptor getTableDescriptor(TableName tableName) throws TableNotFoundException, IOException {
        return null;
    }

    @Override
    public void createTable(HTableDescriptor hTableDescriptor) throws IOException {

    }

    @Override
    public void createTable(HTableDescriptor hTableDescriptor, byte[] bytes, byte[] bytes1, int i) throws IOException {

    }

    @Override
    public void createTable(HTableDescriptor hTableDescriptor, byte[][] bytes) throws IOException {

    }

    @Override
    public void createTableAsync(HTableDescriptor hTableDescriptor, byte[][] bytes) throws IOException {

    }

    @Override
    public void deleteTable(TableName tableName) throws IOException {
        OHConnectionConfiguration connectionConf = new OHConnectionConfiguration(conf);
        ObTableClient tableClient = ObTableClientManager.getOrCreateObTableClientByTableName(tableName, connectionConf);
        OHDeleteTableExecutor executor = new OHDeleteTableExecutor(tableClient);
        try {
            executor.deleteTable(tableName.getNameAsString());
        } catch (IOException e) {
            if (e.getCause() instanceof ObTableTransportException
                && ((ObTableTransportException) e.getCause()).getErrorCode() == TransportCodes.BOLT_TIMEOUT) {
                throw new TimeoutIOException(e.getCause());
            } else {
                throw e;
            }
        }
    }

    @Override
    public HTableDescriptor[] deleteTables(String s) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public void truncateTable(TableName tableName, boolean b) throws IOException {

    }

    @Override
    public void enableTable(TableName tableName) throws IOException {

    }

    @Override
    public void enableTableAsync(TableName tableName) throws IOException {

    }

    @Override
    public HTableDescriptor[] enableTables(String s) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public void disableTableAsync(TableName tableName) throws IOException {

    }

    @Override
    public void disableTable(TableName tableName) throws IOException {

    }

    @Override
    public HTableDescriptor[] disableTables(String s) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public boolean isTableEnabled(TableName tableName) throws IOException {
        return false;
    }

    @Override
    public boolean isTableDisabled(TableName tableName) throws IOException {
        return false;
    }

    @Override
    public boolean isTableAvailable(TableName tableName) throws IOException {
        return false;
    }

    @Override
    public boolean isTableAvailable(TableName tableName, byte[][] bytes) throws IOException {
        return false;
    }

    @Override
    public Pair<Integer, Integer> getAlterStatus(TableName tableName) throws IOException {
        return null;
    }

    @Override
    public Pair<Integer, Integer> getAlterStatus(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public void addColumn(TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {

    }

    @Override
    public void deleteColumn(TableName tableName, byte[] bytes) throws IOException {

    }

    @Override
    public void modifyColumn(TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {

    }

    @Override
    public void closeRegion(String s, String s1) throws IOException {

    }

    @Override
    public void closeRegion(byte[] bytes, String s) throws IOException {

    }

    @Override
    public boolean closeRegionWithEncodedRegionName(String s, String s1) throws IOException {
        return false;
    }

    @Override
    public void closeRegion(ServerName serverName, HRegionInfo hRegionInfo) throws IOException {

    }

    @Override
    public List<HRegionInfo> getOnlineRegions(ServerName serverName) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void flush(TableName tableName) throws IOException {

    }

    @Override
    public void flushRegion(byte[] bytes) throws IOException {

    }

    @Override
    public void compact(TableName tableName) throws IOException {

    }

    @Override
    public void compactRegion(byte[] bytes) throws IOException {

    }

    @Override
    public void compact(TableName tableName, byte[] bytes) throws IOException {

    }

    @Override
    public void compactRegion(byte[] bytes, byte[] bytes1) throws IOException {

    }

    @Override
    public void majorCompact(TableName tableName) throws IOException {

    }

    @Override
    public void majorCompactRegion(byte[] bytes) throws IOException {

    }

    @Override
    public void majorCompact(TableName tableName, byte[] bytes) throws IOException {

    }

    @Override
    public void majorCompactRegion(byte[] bytes, byte[] bytes1) throws IOException {

    }

    @Override
    public void compactRegionServer(ServerName serverName, boolean b) throws IOException, InterruptedException {

    }

    @Override
    public void move(byte[] bytes, byte[] bytes1) throws IOException {

    }

    @Override
    public void assign(byte[] bytes) throws IOException {

    }

    @Override
    public void unassign(byte[] bytes, boolean b) throws IOException {

    }

    @Override
    public void offline(byte[] bytes) throws IOException {

    }

    @Override
    public boolean setBalancerRunning(boolean b, boolean b1) throws IOException {
        return false;
    }

    @Override
    public boolean balancer() throws IOException {
        return false;
    }

    @Override
    public boolean balancer(boolean b) throws IOException {
        return false;
    }

    @Override
    public boolean isBalancerEnabled() throws IOException {
        return false;
    }

    @Override
    public boolean normalize() throws IOException {
        return false;
    }

    @Override
    public boolean isNormalizerEnabled() throws IOException {
        return false;
    }

    @Override
    public boolean setNormalizerRunning(boolean b) throws IOException {
        return false;
    }

    @Override
    public boolean enableCatalogJanitor(boolean b) throws IOException {
        return false;
    }

    @Override
    public int runCatalogScan() throws IOException {
        return 0;
    }

    @Override
    public boolean isCatalogJanitorEnabled() throws IOException {
        return false;
    }

    @Override
    public void mergeRegions(byte[] bytes, byte[] bytes1, boolean b) throws IOException {

    }

    @Override
    public void split(TableName tableName) throws IOException {

    }

    @Override
    public void splitRegion(byte[] bytes) throws IOException {

    }

    @Override
    public void split(TableName tableName, byte[] bytes) throws IOException {

    }

    @Override
    public void splitRegion(byte[] bytes, byte[] bytes1) throws IOException {

    }

    @Override
    public void modifyTable(TableName tableName, HTableDescriptor hTableDescriptor) throws IOException {

    }

    @Override
    public void shutdown() throws IOException {

    }

    @Override
    public void stopMaster() throws IOException {

    }

    @Override
    public void stopRegionServer(String s) throws IOException {

    }

    @Override
    public ClusterStatus getClusterStatus() throws IOException {
        return null;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

    @Override
    public void createNamespace(NamespaceDescriptor namespaceDescriptor) throws IOException {

    }

    @Override
    public void modifyNamespace(NamespaceDescriptor namespaceDescriptor) throws IOException {

    }

    @Override
    public void deleteNamespace(String s) throws IOException {

    }

    @Override
    public NamespaceDescriptor getNamespaceDescriptor(String s) throws NamespaceNotFoundException, IOException {
        return null;
    }

    @Override
    public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
        return new NamespaceDescriptor[0];
    }

    @Override
    public HTableDescriptor[] listTableDescriptorsByNamespace(String s) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public TableName[] listTableNamesByNamespace(String s) throws IOException {
        return new TableName[0];
    }

    @Override
    public List<HRegionInfo> getTableRegions(TableName tableName) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public HTableDescriptor[] getTableDescriptorsByTableName(List<TableName> list) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public HTableDescriptor[] getTableDescriptors(List<String> list) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public boolean abortProcedure(long l, boolean b) throws IOException {
        return false;
    }

    @Override
    public ProcedureInfo[] listProcedures() throws IOException {
        return new ProcedureInfo[0];
    }

    @Override
    public Future<Boolean> abortProcedureAsync(long l, boolean b) throws IOException {
        return null;
    }

    @Override
    public void rollWALWriter(ServerName serverName) throws IOException, FailedLogCloseException {

    }

    @Override
    public String[] getMasterCoprocessors() throws IOException {
        return new String[0];
    }

    @Override
    public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionState(TableName tableName) throws IOException {
        return null;
    }

    @Override
    public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionStateForRegion(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public long getLastMajorCompactionTimestamp(TableName tableName) throws IOException {
        return 0;
    }

    @Override
    public long getLastMajorCompactionTimestampForRegion(byte[] bytes) throws IOException {
        return 0;
    }

    @Override
    public void snapshot(String s, TableName tableName) throws IOException, SnapshotCreationException, IllegalArgumentException {

    }

    @Override
    public void snapshot(byte[] bytes, TableName tableName) throws IOException, SnapshotCreationException, IllegalArgumentException {

    }

    @Override
    public void snapshot(String s, TableName tableName, HBaseProtos.SnapshotDescription.Type type) throws IOException, SnapshotCreationException, IllegalArgumentException {

    }

    @Override
    public void snapshot(HBaseProtos.SnapshotDescription snapshotDescription) throws IOException, SnapshotCreationException, IllegalArgumentException {

    }

    @Override
    public MasterProtos.SnapshotResponse takeSnapshotAsync(HBaseProtos.SnapshotDescription snapshotDescription) throws IOException, SnapshotCreationException {
        return null;
    }

    @Override
    public boolean isSnapshotFinished(HBaseProtos.SnapshotDescription snapshotDescription) throws IOException, HBaseSnapshotException, UnknownSnapshotException {
        return false;
    }

    @Override
    public void restoreSnapshot(byte[] bytes) throws IOException, RestoreSnapshotException {

    }

    @Override
    public void restoreSnapshot(String s) throws IOException, RestoreSnapshotException {

    }

    @Override
    public void restoreSnapshot(byte[] bytes, boolean b) throws IOException, RestoreSnapshotException {

    }

    @Override
    public void restoreSnapshot(String s, boolean b) throws IOException, RestoreSnapshotException {

    }

    @Override
    public void cloneSnapshot(byte[] bytes, TableName tableName) throws IOException, TableExistsException, RestoreSnapshotException {

    }

    @Override
    public void cloneSnapshot(String s, TableName tableName) throws IOException, TableExistsException, RestoreSnapshotException {

    }

    @Override
    public void execProcedure(String s, String s1, Map<String, String> map) throws IOException {

    }

    @Override
    public byte[] execProcedureWithRet(String s, String s1, Map<String, String> map) throws IOException {
        return new byte[0];
    }

    @Override
    public boolean isProcedureFinished(String s, String s1, Map<String, String> map) throws IOException {
        return false;
    }

    @Override
    public List<HBaseProtos.SnapshotDescription> listSnapshots() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<HBaseProtos.SnapshotDescription> listSnapshots(String s) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<HBaseProtos.SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<HBaseProtos.SnapshotDescription> listTableSnapshots(String s, String s1) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<HBaseProtos.SnapshotDescription> listTableSnapshots(Pattern pattern, Pattern pattern1) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void deleteSnapshot(byte[] bytes) throws IOException {

    }

    @Override
    public void deleteSnapshot(String s) throws IOException {

    }

    @Override
    public void deleteSnapshots(String s) throws IOException {

    }

    @Override
    public void deleteSnapshots(Pattern pattern) throws IOException {

    }

    @Override
    public void deleteTableSnapshots(String s, String s1) throws IOException {

    }

    @Override
    public void deleteTableSnapshots(Pattern pattern, Pattern pattern1) throws IOException {

    }

    @Override
    public void setQuota(QuotaSettings quotaSettings) throws IOException {

    }

    @Override
    public QuotaRetriever getQuotaRetriever(QuotaFilter quotaFilter) throws IOException {
        return null;
    }

    @Override
    public CoprocessorRpcChannel coprocessorService() {
        return null;
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(ServerName serverName) {
        return null;
    }

    @Override
    public void updateConfiguration(ServerName serverName) throws IOException {

    }

    @Override
    public void updateConfiguration() throws IOException {

    }

    @Override
    public int getMasterInfoPort() throws IOException {
        return 0;
    }

    @Override
    public List<SecurityCapability> getSecurityCapabilities() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public boolean[] setSplitOrMergeEnabled(boolean b, boolean b1, MasterSwitchType... masterSwitchTypes) throws IOException {
        return new boolean[0];
    }

    @Override
    public boolean isSplitOrMergeEnabled(MasterSwitchType masterSwitchType) throws IOException {
        return false;
    }
}
