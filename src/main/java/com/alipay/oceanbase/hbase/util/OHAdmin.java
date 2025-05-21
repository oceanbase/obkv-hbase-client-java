package com.alipay.oceanbase.hbase.util;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.bolt.transport.TransportCodes;
import com.alipay.oceanbase.rpc.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.rpc.exception.ObTableTransportException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

public class OHAdmin implements Admin {
    private final ObTableClient tableClient;
    OHAdmin(ObTableClient tableClient) {
        this.tableClient = tableClient;
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
        OHTableExistsExecutor executor = new OHTableExistsExecutor(tableClient);
        return executor.tableExists(tableName.getNameAsString());
    }

    @Override
    public HTableDescriptor[] listTables() throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public List<TableDescriptor> listTableDescriptors() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
        return new HTableDescriptor[0];
    }

    @Override
    public List<TableDescriptor> listTableDescriptors(Pattern pattern) throws IOException {
        return Collections.emptyList();
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
    public List<TableDescriptor> listTableDescriptors(Pattern pattern, boolean b) throws IOException {
        return Collections.emptyList();
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
    public TableDescriptor getDescriptor(TableName tableName) throws TableNotFoundException, IOException {
        return null;
    }

    @Override
    public void createTable(TableDescriptor tableDescriptor) throws IOException {

    }

    @Override
    public void createTable(TableDescriptor tableDescriptor, byte[] bytes, byte[] bytes1, int i) throws IOException {

    }

    @Override
    public void createTable(TableDescriptor tableDescriptor, byte[][] bytes) throws IOException {

    }

    @Override
    public Future<Void> createTableAsync(TableDescriptor tableDescriptor, byte[][] bytes) throws IOException {
        return null;
    }

    @Override
    public void deleteTable(TableName tableName) throws IOException {
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
    public Future<Void> deleteTableAsync(TableName tableName) throws IOException {
        return null;
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
    public Future<Void> truncateTableAsync(TableName tableName, boolean b) throws IOException {
        return null;
    }

    @Override
    public void enableTable(TableName tableName) throws IOException {

    }

    @Override
    public Future<Void> enableTableAsync(TableName tableName) throws IOException {
        return null;
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
    public Future<Void> disableTableAsync(TableName tableName) throws IOException {
        return null;
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
    public void addColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamilyDescriptor) throws IOException {

    }

    @Override
    public Future<Void> addColumnFamilyAsync(TableName tableName, ColumnFamilyDescriptor columnFamilyDescriptor) throws IOException {
        return null;
    }

    @Override
    public void deleteColumn(TableName tableName, byte[] bytes) throws IOException {

    }

    @Override
    public void deleteColumnFamily(TableName tableName, byte[] bytes) throws IOException {

    }

    @Override
    public Future<Void> deleteColumnFamilyAsync(TableName tableName, byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public void modifyColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamilyDescriptor) throws IOException {

    }

    @Override
    public Future<Void> modifyColumnFamilyAsync(TableName tableName, ColumnFamilyDescriptor columnFamilyDescriptor) throws IOException {
        return null;
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
    public List<RegionInfo> getRegions(ServerName serverName) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void flush(TableName tableName) throws IOException {

    }

    @Override
    public void flushRegion(byte[] bytes) throws IOException {

    }

    @Override
    public void flushRegionServer(ServerName serverName) throws IOException {

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
    public void compact(TableName tableName, CompactType compactType) throws IOException, InterruptedException {

    }

    @Override
    public void compact(TableName tableName, byte[] bytes, CompactType compactType) throws IOException, InterruptedException {

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
    public void majorCompact(TableName tableName, CompactType compactType) throws IOException, InterruptedException {

    }

    @Override
    public void majorCompact(TableName tableName, byte[] bytes, CompactType compactType) throws IOException, InterruptedException {

    }

    @Override
    public void compactRegionServer(ServerName serverName) throws IOException {

    }

    @Override
    public void majorCompactRegionServer(ServerName serverName) throws IOException {

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
    public boolean balancerSwitch(boolean b, boolean b1) throws IOException {
        return false;
    }

    @Override
    public boolean balance() throws IOException {
        return false;
    }

    @Override
    public boolean balance(boolean b) throws IOException {
        return false;
    }

    @Override
    public boolean isBalancerEnabled() throws IOException {
        return false;
    }

    @Override
    public CacheEvictionStats clearBlockCache(TableName tableName) throws IOException {
        return null;
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
    public boolean normalizerSwitch(boolean b) throws IOException {
        return false;
    }

    @Override
    public boolean catalogJanitorSwitch(boolean b) throws IOException {
        return false;
    }

    @Override
    public int runCatalogJanitor() throws IOException {
        return 0;
    }

    @Override
    public boolean isCatalogJanitorEnabled() throws IOException {
        return false;
    }

    @Override
    public boolean cleanerChoreSwitch(boolean b) throws IOException {
        return false;
    }

    @Override
    public boolean runCleanerChore() throws IOException {
        return false;
    }

    @Override
    public boolean isCleanerChoreEnabled() throws IOException {
        return false;
    }

    @Override
    public void mergeRegions(byte[] bytes, byte[] bytes1, boolean b) throws IOException {

    }

    @Override
    public Future<Void> mergeRegionsAsync(byte[] bytes, byte[] bytes1, boolean b) throws IOException {
        return null;
    }

    @Override
    public Future<Void> mergeRegionsAsync(byte[][] bytes, boolean b) throws IOException {
        return null;
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
    public Future<Void> splitRegionAsync(byte[] bytes, byte[] bytes1) throws IOException {
        return null;
    }

    @Override
    public void modifyTable(TableName tableName, TableDescriptor tableDescriptor) throws IOException {

    }

    @Override
    public void modifyTable(TableDescriptor tableDescriptor) throws IOException {

    }

    @Override
    public Future<Void> modifyTableAsync(TableName tableName, TableDescriptor tableDescriptor) throws IOException {
        return null;
    }

    @Override
    public Future<Void> modifyTableAsync(TableDescriptor tableDescriptor) throws IOException {
        return null;
    }

    @Override
    public void shutdown() throws IOException {

    }

    @Override
    public void stopMaster() throws IOException {

    }

    @Override
    public boolean isMasterInMaintenanceMode() throws IOException {
        return false;
    }

    @Override
    public void stopRegionServer(String s) throws IOException {

    }

    @Override
    public ClusterMetrics getClusterMetrics(EnumSet<ClusterMetrics.Option> enumSet) throws IOException {
        return null;
    }

    @Override
    public List<RegionMetrics> getRegionMetrics(ServerName serverName) throws IOException {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public List<RegionMetrics> getRegionMetrics(ServerName serverName, TableName tableName) throws IOException {
        OHRegionMetricsExecutor executor = new OHRegionMetricsExecutor(tableClient);
        if (tableName == null) {
            throw new FeatureNotSupportedException("does not support tableName is null");
        }
        return executor.getRegionMetrics(tableName.getNameAsString());
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

    @Override
    public void createNamespace(NamespaceDescriptor namespaceDescriptor) throws IOException {

    }

    @Override
    public Future<Void> createNamespaceAsync(NamespaceDescriptor namespaceDescriptor) throws IOException {
        return null;
    }

    @Override
    public void modifyNamespace(NamespaceDescriptor namespaceDescriptor) throws IOException {

    }

    @Override
    public Future<Void> modifyNamespaceAsync(NamespaceDescriptor namespaceDescriptor) throws IOException {
        return null;
    }

    @Override
    public void deleteNamespace(String s) throws IOException {

    }

    @Override
    public Future<Void> deleteNamespaceAsync(String s) throws IOException {
        return null;
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
    public List<TableDescriptor> listTableDescriptorsByNamespace(byte[] bytes) throws IOException {
        return Collections.emptyList();
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
    public List<RegionInfo> getRegions(TableName tableName) throws IOException {
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
    public List<TableDescriptor> listTableDescriptors(List<TableName> list) throws IOException {
        return Collections.emptyList();
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
    public Future<Boolean> abortProcedureAsync(long l, boolean b) throws IOException {
        return null;
    }

    @Override
    public String getProcedures() throws IOException {
        return "";
    }

    @Override
    public String getLocks() throws IOException {
        return "";
    }

    @Override
    public void rollWALWriter(ServerName serverName) throws IOException, FailedLogCloseException {

    }

    @Override
    public CompactionState getCompactionState(TableName tableName) throws IOException {
        return null;
    }

    @Override
    public CompactionState getCompactionState(TableName tableName, CompactType compactType) throws IOException {
        return null;
    }

    @Override
    public CompactionState getCompactionStateForRegion(byte[] bytes) throws IOException {
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
    public void snapshot(String s, TableName tableName, SnapshotType snapshotType) throws IOException, SnapshotCreationException, IllegalArgumentException {

    }

    @Override
    public void snapshot(SnapshotDescription snapshotDescription) throws IOException, SnapshotCreationException, IllegalArgumentException {

    }

    @Override
    public void snapshotAsync(SnapshotDescription snapshotDescription) throws IOException, SnapshotCreationException {

    }

    @Override
    public boolean isSnapshotFinished(SnapshotDescription snapshotDescription) throws IOException, HBaseSnapshotException, UnknownSnapshotException {
        return false;
    }

    @Override
    public void restoreSnapshot(byte[] bytes) throws IOException, RestoreSnapshotException {

    }

    @Override
    public void restoreSnapshot(String s) throws IOException, RestoreSnapshotException {

    }

    @Override
    public Future<Void> restoreSnapshotAsync(String s) throws IOException, RestoreSnapshotException {
        return null;
    }

    @Override
    public void restoreSnapshot(byte[] bytes, boolean b) throws IOException, RestoreSnapshotException {

    }

    @Override
    public void restoreSnapshot(String s, boolean b) throws IOException, RestoreSnapshotException {

    }

    @Override
    public void restoreSnapshot(String s, boolean b, boolean b1) throws IOException, RestoreSnapshotException {

    }

    @Override
    public void cloneSnapshot(byte[] bytes, TableName tableName) throws IOException, TableExistsException, RestoreSnapshotException {

    }

    @Override
    public void cloneSnapshot(String s, TableName tableName, boolean b) throws IOException, TableExistsException, RestoreSnapshotException {

    }

    @Override
    public void cloneSnapshot(String s, TableName tableName) throws IOException, TableExistsException, RestoreSnapshotException {

    }

    @Override
    public Future<Void> cloneSnapshotAsync(String s, TableName tableName) throws IOException, TableExistsException {
        return null;
    }

    @Override
    public void execProcedure(String s, String s1, Map<String, String> map) throws IOException {

    }

    @Override
    public byte[] execProcedureWithReturn(String s, String s1, Map<String, String> map) throws IOException {
        return new byte[0];
    }

    @Override
    public boolean isProcedureFinished(String s, String s1, Map<String, String> map) throws IOException {
        return false;
    }

    @Override
    public List<SnapshotDescription> listSnapshots() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<SnapshotDescription> listSnapshots(String s) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<SnapshotDescription> listTableSnapshots(String s, String s1) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<SnapshotDescription> listTableSnapshots(Pattern pattern, Pattern pattern1) throws IOException {
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
    public List<QuotaSettings> getQuota(QuotaFilter quotaFilter) throws IOException {
        return Collections.emptyList();
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
    public List<SecurityCapability> getSecurityCapabilities() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public boolean splitSwitch(boolean b, boolean b1) throws IOException {
        return false;
    }

    @Override
    public boolean mergeSwitch(boolean b, boolean b1) throws IOException {
        return false;
    }

    @Override
    public boolean isSplitEnabled() throws IOException {
        return false;
    }

    @Override
    public boolean isMergeEnabled() throws IOException {
        return false;
    }

    @Override
    public void addReplicationPeer(String s, ReplicationPeerConfig replicationPeerConfig, boolean b) throws IOException {

    }

    @Override
    public void removeReplicationPeer(String s) throws IOException {

    }

    @Override
    public void enableReplicationPeer(String s) throws IOException {

    }

    @Override
    public void disableReplicationPeer(String s) throws IOException {

    }

    @Override
    public ReplicationPeerConfig getReplicationPeerConfig(String s) throws IOException {
        return null;
    }

    @Override
    public void updateReplicationPeerConfig(String s, ReplicationPeerConfig replicationPeerConfig) throws IOException {

    }

    @Override
    public void appendReplicationPeerTableCFs(String s, Map<TableName, List<String>> map) throws ReplicationException, IOException {

    }

    @Override
    public void removeReplicationPeerTableCFs(String s, Map<TableName, List<String>> map) throws ReplicationException, IOException {

    }

    @Override
    public List<ReplicationPeerDescription> listReplicationPeers() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<ReplicationPeerDescription> listReplicationPeers(Pattern pattern) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void decommissionRegionServers(List<ServerName> list, boolean b) throws IOException {

    }

    @Override
    public List<ServerName> listDecommissionedRegionServers() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void recommissionRegionServer(ServerName serverName, List<byte[]> list) throws IOException {

    }

    @Override
    public List<TableCFs> listReplicatedTableCFs() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void enableTableReplication(TableName tableName) throws IOException {

    }

    @Override
    public void disableTableReplication(TableName tableName) throws IOException {

    }

    @Override
    public void clearCompactionQueues(ServerName serverName, Set<String> set) throws IOException, InterruptedException {

    }

    @Override
    public List<ServerName> clearDeadServers(List<ServerName> list) throws IOException {
        return Collections.emptyList();
    }
}
