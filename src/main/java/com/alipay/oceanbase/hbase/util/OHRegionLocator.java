package com.alipay.oceanbase.hbase.util;

import com.alipay.oceanbase.rpc.ObTableClient;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class OHRegionLocator implements RegionLocator {
    private byte[][] startKeys;
    private byte[][] endKeys;
    private TableName tableName;

    private List<HRegionLocation> regionLocations;

    public OHRegionLocator(byte[][] startKeys, byte[][] endKeys, List<HRegionLocation> regionLocations) {
        this.startKeys = startKeys;
        this.endKeys = endKeys;
        this.regionLocations = regionLocations;
    }

    @Override
    public HRegionLocation getRegionLocation(byte[] bytes) throws IOException {
        // check if bytes is in the range of startKeys and endKeys
        for (HRegionLocation regionLocation : regionLocations) {
            if (regionLocation.getRegionInfo().containsRow(bytes)) {
                return regionLocation;
            }
        }
        return null;
    }

    @Override
    public HRegionLocation getRegionLocation(byte[] bytes, boolean b) throws IOException {
        if (b) {
            OHRegionLocatorExecutor executor = new OHRegionLocatorExecutor(tableName.toString(), tableClient);
            RegionLocator location = executor.getRegionLocator(tableName.toString());
            this.startKeys = location.getStartKeys();
            this.endKeys = location.getEndKeys();
            this.regionLocations = location.getAllRegionLocations();
        }
        return getRegionLocation(bytes);
    }

    @Override
    public List<HRegionLocation> getAllRegionLocations() throws IOException {
        return regionLocations;
    }

    /**
     * Gets the starting row key for every region in the currently open table.
     * <p>
     * This is mainly useful for the MapReduce integration.
     *
     * @return Array of region starting row keys
     * @throws IOException if a remote or network exception occurs
     */
    @Override
    public byte[][] getStartKeys() throws IOException {
        return startKeys;
    }

    /**
     * Gets the ending row key for every region in the currently open table.
     * <p>
     * This is mainly useful for the MapReduce integration.
     *
     * @return Array of region ending row keys
     * @throws IOException if a remote or network exception occurs
     */
    @Override
    public byte[][] getEndKeys() throws IOException {
        return endKeys;
    }

    /**
     * Gets the starting and ending row keys for every region in the currently
     * open table.
     * <p>
     * This is mainly useful for the MapReduce integration.
     *
     * @return Pair of arrays of region starting and ending row keys
     * @throws IOException if a remote or network exception occurs
     */
    @Override
    public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
        return Pair.newPair(startKeys, endKeys);
    }

    @Override
    public TableName getName() {
        return tableName;
    }

    private ObTableClient tableClient;

    @Override
    public void close() throws IOException {
        return;
    }
}