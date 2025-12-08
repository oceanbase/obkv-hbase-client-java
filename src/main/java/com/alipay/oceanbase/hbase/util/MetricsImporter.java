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

public class MetricsImporter {
    private boolean isFailedOp;
    private long    duration;
    private long    singleOpCount;

    public MetricsImporter() {
        this.isFailedOp = false;
        this.duration = 0;
        this.singleOpCount = 0;
    }

    public void setIsFailedOp(boolean isFailedOp) {
        this.isFailedOp = isFailedOp;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public void setSingleOpCount(long singleOpCount) {
        this.singleOpCount = singleOpCount;
    }

    public boolean isFailedOp() {
        return isFailedOp;
    }

    public long getDuration() {
        return duration;
    }

    public long getSingleOpCount() {
        return singleOpCount;
    }
}
