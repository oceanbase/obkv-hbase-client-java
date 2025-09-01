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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.ObTableClient;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@InterfaceAudience.Private
public class OHBaseFuncUtils {
    public static byte[][] extractFamilyFromQualifier(byte[] qualifier) throws Exception {
        int familyLen = -1;
        for (int i = 0; i < qualifier.length; i++) {
            if (qualifier[i] == '\0') {
                familyLen = i;
                break;
            }
        }
        if (familyLen == -1) {
            throw new RuntimeException("Cannot get family name");
        }
        byte[] family = Arrays.copyOfRange(qualifier, 0, familyLen);
        byte[] newQualifier = Arrays.copyOfRange(qualifier, familyLen + 1, qualifier.length);
        return new byte[][] { family, newQualifier };
    }

    public static boolean isHBasePutPefSupport(ObTableClient tableClient) {
        if (tableClient.isOdpMode()) {
            // server version support and distributed capacity is enabled and odp version support
            return ObGlobal.isHBasePutPerfSupport()
                    && tableClient.getServerCapacity().isSupportDistributedExecute()
                    && ObGlobal.OB_PROXY_VERSION >= ObGlobal.OB_PROXY_VERSION_4_3_6_0;
        } else {
            // server version support and distributed capacity is enabled
            return ObGlobal.isHBasePutPerfSupport()
                    && tableClient.getServerCapacity().isSupportDistributedExecute();
        }
    }

    public static boolean isAllPut(List<? extends Row> actions) {
        boolean isAllPut = true;
        for (Row action : actions) {
            if (!(action instanceof Put)) {
                isAllPut = false;
                break;
            }
        }
        return isAllPut;
    }

    public static void sortHBaseResult(List<KeyValue> cells) {
        cells.sort(new Comparator<KeyValue>() {
            @Override
            public int compare(KeyValue kv1, KeyValue kv2) {
                // 1. sort family in lexicographical order
                int familyComparison = Bytes.compareTo(kv1.getFamilyArray(),
                        kv1.getFamilyOffset(), kv1.getFamilyLength(), kv2.getFamilyArray(),
                        kv2.getFamilyOffset(), kv2.getFamilyLength());
                if (familyComparison != 0) {
                    return familyComparison;
                }

                // 2: sort qualifier in lexicographical order
                int qualifierComparison = Bytes.compareTo(kv1.getQualifierArray(),
                        kv1.getQualifierOffset(), kv1.getQualifierLength(),
                        kv2.getQualifierArray(), kv2.getQualifierOffset(),
                        kv2.getQualifierLength());
                if (qualifierComparison != 0) {
                    return qualifierComparison;
                }

                // 3: sort timestamp in descend order
                return Long.compare(kv2.getTimestamp(), kv1.getTimestamp());
            }
        });
    }

    public static boolean serverCanRetry(ObTableClient tableClient) {
        if (tableClient.isOdpMode()) {
            // ODP mode needs to check proxy version
            return ObGlobal.OB_PROXY_VERSION >= ObGlobal.OB_PROXY_VERSION_4_3_6_0;
        } else {
            // OCP mode directly return true, server will do the check
            return true;
        }
    }
}
