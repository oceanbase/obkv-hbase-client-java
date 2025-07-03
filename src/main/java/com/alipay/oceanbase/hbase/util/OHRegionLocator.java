/*-
 * #%L
 * com.oceanbase:obkv-hbase-client
 * %%
 * Copyright (C) 2022 - 2025 OceanBase Group
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

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.bolt.transport.TransportCodes;
import com.alipay.oceanbase.rpc.exception.ObTableTransportException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class OHRegionLocator implements RegionLocator {
    private byte[][]                             startKeys;
    private byte[][]                             endKeys;
    private final ObTableClient                  tableClient;
    private final TableName                      tableName;

    private List<Pair<HRegionLocation, Boolean>> regionLocations;

    public OHRegionLocator(byte[][] startKeys, byte[][] endKeys,
                           List<Pair<HRegionLocation, Boolean>> regionLocations,
                           TableName tableName, ObTableClient tableClient) {
        this.startKeys = startKeys;
        this.endKeys = endKeys;
        this.regionLocations = regionLocations;
        this.tableName = tableName;
        this.tableClient = tableClient;
    }

    @Override
    public HRegionLocation getRegionLocation(byte[] bytes) throws IOException {
        // check if bytes is in the range of startKeys and endKeys
        for (Pair<HRegionLocation, Boolean> pair : regionLocations) {
            if (pair.getSecond() && pair.getFirst().getRegionInfo().containsRow(bytes)) {
                return pair.getFirst();
            }
        }
        return null;
    }

    public List<Pair<HRegionLocation, Boolean>> getRegionLocationPair() {
        return regionLocations;
    }

    @Override
    public HRegionLocation getRegionLocation(byte[] bytes, boolean b) throws IOException {
        if (b || regionLocations.isEmpty()) {
            OHRegionLocatorExecutor executor = new OHRegionLocatorExecutor(tableName.toString(),
                tableClient);
            try {
                OHRegionLocator location = executor.getRegionLocator(tableName.toString());
                this.startKeys = location.getStartKeys();
                this.endKeys = location.getEndKeys();
                this.regionLocations = location.getRegionLocationPair();
            } catch (IOException e) {
                if (e.getCause() instanceof ObTableTransportException
                    && ((ObTableTransportException) e.getCause()).getErrorCode() == TransportCodes.BOLT_TIMEOUT) {
                    throw new TimeoutIOException(e.getCause());
                } else {
                    throw e;
                }
            }
        }
        return getRegionLocation(bytes);
    }

    @Override
    public List<HRegionLocation> getAllRegionLocations() throws IOException {
        return regionLocations.stream().map(Pair::getFirst).collect(Collectors.toList());
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

    @Override
    public void close() throws IOException {
        return;
    }
}
