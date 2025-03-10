/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2022 OceanBase Group
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

public class TimeGenerator {
    public static class TimeRange {
        private final long lowerBound1;
        private final long lowerBound2;
        private final long upperBound1;
        private final long upperBound2;

        public TimeRange(long lowerBound1, long lowerBound2, long upperBound1, long upperBound2) {
            this.lowerBound1 = lowerBound1;
            this.lowerBound2 = lowerBound2;
            this.upperBound1 = upperBound1;
            this.upperBound2 = upperBound2;
        }

        public long lowerBound1() { return lowerBound1; }
        public long lowerBound2() { return lowerBound2; }
        public long upperBound1() { return upperBound1; }
        public long upperBound2() { return upperBound2; }
    }

    public static TimeRange generateTestTimeRange() {
        long current = System.currentTimeMillis();
        return new TimeRange(
                current - 86400000,  // 24小时前
                current,
                current + 86400000,  // 24小时后
                current + 172800000  // 48小时后
        );
    }
}
