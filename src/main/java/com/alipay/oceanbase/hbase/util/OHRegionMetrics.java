package com.alipay.oceanbase.hbase.util;

import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.Size;

import java.util.Collections;
import java.util.Map;

public class OHRegionMetrics implements RegionMetrics {
    private final String tablegroup;
    private final byte[] name; // tablet_name, id in String
    private final Size storeFileSize; // tablet storage used in ssTable
    private final Size memStoreSize; // tablet storage used in memTable

    OHRegionMetrics(String tablegroup, byte[] name, Size storeFileSize, Size memStoreSize) {
        this.tablegroup = tablegroup;
        this.name = name;
        this.storeFileSize = storeFileSize;
        this.memStoreSize = memStoreSize;
    }

    public String getTablegroup() {
        return tablegroup;
    }

    @Override
    public byte[] getRegionName() {
        return name;
    }

    @Override
    public int getStoreCount() {
        return 0;
    }

    @Override
    public int getStoreFileCount() {
        return 0;
    }

    @Override
    public Size getStoreFileSize() {
        return storeFileSize;
    }

    @Override
    public Size getMemStoreSize() {
        return memStoreSize;
    }

    @Override
    public long getReadRequestCount() {
        return 0;
    }

    @Override
    public long getWriteRequestCount() {
        return 0;
    }

    @Override
    public long getFilteredReadRequestCount() {
        return 0;
    }

    @Override
    public Size getStoreFileIndexSize() {
        return null;
    }

    @Override
    public Size getStoreFileRootLevelIndexSize() {
        return null;
    }

    @Override
    public Size getStoreFileUncompressedDataIndexSize() {
        return null;
    }

    @Override
    public Size getBloomFilterSize() {
        return null;
    }

    @Override
    public long getCompactingCellCount() {
        return 0;
    }

    @Override
    public long getCompactedCellCount() {
        return 0;
    }

    @Override
    public long getCompletedSequenceId() {
        return 0;
    }

    @Override
    public Map<byte[], Long> getStoreSequenceId() {
        return Collections.emptyMap();
    }

    @Override
    public Size getUncompressedStoreFileSize() {
        return null;
    }

    @Override
    public float getDataLocality() {
        return 0;
    }

    @Override
    public long getLastMajorCompactionTimestamp() {
        return 0;
    }
}
