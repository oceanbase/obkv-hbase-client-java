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
    public OHRegionLocator(byte[][] startKeys, byte[][] endKeys) {
        
    }

    @Override
    public HRegionLocation getRegionLocation(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public HRegionLocation getRegionLocation(byte[] bytes, boolean b) throws IOException {
        return null;
    }

    @Override
    public List<HRegionLocation> getAllRegionLocations() throws IOException {
        return Collections.emptyList();
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
        return null;
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
        return null;
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
        return null;
    }

    @Override
    public TableName getName() {
        return null;
    }

    private ObTableClient tableClient;

    @Override
    public void close() throws IOException {

    }
}
