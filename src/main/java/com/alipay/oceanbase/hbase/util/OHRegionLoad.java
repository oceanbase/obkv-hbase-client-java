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

import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;

public class OHRegionLoad extends RegionLoad {
    private final byte[] name;         // tablet_name, id in String
    private int          storeFileSize; // tablet storage used in ssTable
    private int          memStoreSize; // tablet storage used in memTable

    public OHRegionLoad(byte[] name, int storeFileSize, int memStoreSize) {
        super(null);
        this.name = name;
        this.storeFileSize = storeFileSize;
        this.memStoreSize = memStoreSize;
    }

    @Override
    public byte[] getName() {
        return name;
    }

    /**
     * @return the number of stores
     */
    @Override
    public int getStores() {
        return 1;
    }

    /**
     * @return the number of storefiles
     */
    @Override
    public int getStorefiles() {
        return 1;
    }

    /**
     * @return the total size of the storefiles, in MB
     */
    @Override
    public int getStorefileSizeMB() {
        return memStoreSize + storeFileSize / 1024 / 1024;
    }
}
