package com.alipay.oceanbase.hbase.util;

import com.alipay.oceanbase.hbase.exception.FeatureNotSupportedException;
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
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public int getStoreFileCount() {
        throw new FeatureNotSupportedException("does not support yet");
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
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public long getWriteRequestCount() {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public long getFilteredReadRequestCount() {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public Size getStoreFileIndexSize() {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public Size getStoreFileRootLevelIndexSize() {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public Size getStoreFileUncompressedDataIndexSize() {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public Size getBloomFilterSize() {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public long getCompactingCellCount() {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public long getCompactedCellCount() {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public long getCompletedSequenceId() {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public Map<byte[], Long> getStoreSequenceId() {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public Size getUncompressedStoreFileSize() {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public float getDataLocality() {
        throw new FeatureNotSupportedException("does not support yet");
    }

    @Override
    public long getLastMajorCompactionTimestamp() {
        throw new FeatureNotSupportedException("does not support yet");
    }
}
