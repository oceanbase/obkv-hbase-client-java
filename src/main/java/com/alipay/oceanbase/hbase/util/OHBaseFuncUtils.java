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
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.OHOperationType;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@InterfaceAudience.Private
public class OHBaseFuncUtils {
    private static final Comparator<Cell> HBASE_CELL_COMPARATOR = new Comparator<Cell>() {
                                                                    @Override
                                                                    public int compare(Cell cell1,
                                                                                       Cell cell2) {
                                                                        int familyComparison = Bytes
                                                                            .compareTo(
                                                                                cell1
                                                                                    .getFamilyArray(),
                                                                                cell1
                                                                                    .getFamilyOffset(),
                                                                                cell1
                                                                                    .getFamilyLength(),
                                                                                cell2
                                                                                    .getFamilyArray(),
                                                                                cell2
                                                                                    .getFamilyOffset(),
                                                                                cell2
                                                                                    .getFamilyLength());
                                                                        if (familyComparison != 0) {
                                                                            return familyComparison;
                                                                        }

                                                                        int qualifierComparison = Bytes
                                                                            .compareTo(
                                                                                cell1
                                                                                    .getQualifierArray(),
                                                                                cell1
                                                                                    .getQualifierOffset(),
                                                                                cell1
                                                                                    .getQualifierLength(),
                                                                                cell2
                                                                                    .getQualifierArray(),
                                                                                cell2
                                                                                    .getQualifierOffset(),
                                                                                cell2
                                                                                    .getQualifierLength());
                                                                        if (qualifierComparison != 0) {
                                                                            return qualifierComparison;
                                                                        }

                                                                        return Long.compare(
                                                                            cell2.getTimestamp(),
                                                                            cell1.getTimestamp());
                                                                    }
                                                                };

    /**
     * Build a TableGroup KeyValue directly from the protocol's {@code family\0qualifier} column.
     * The offset constructor copies both ranges into the final KeyValue backing array and avoids
     * allocating temporary family and qualifier arrays.
     */
    public static KeyValue createTableGroupKeyValue(byte[] row, byte[] familyQualifier,
                                                    long timestamp, byte[] value) {
        int familyLength = findFamilyDelimiter(familyQualifier);
        int qualifierOffset = familyLength + 1;
        return new KeyValue(row, 0, row == null ? 0 : row.length, familyQualifier, 0, familyLength,
            familyQualifier, qualifierOffset, familyQualifier.length - qualifierOffset, timestamp,
            KeyValue.Type.Put, value, 0, value == null ? 0 : value.length);
    }

    private static int findFamilyDelimiter(byte[] familyQualifier) {
        for (int i = 0; i < familyQualifier.length; i++) {
            if (familyQualifier[i] == '\0') {
                return i;
            }
        }
        // Keep the failure contract of extractFamilyFromQualifier for malformed responses.
        throw new RuntimeException("Cannot get family name");
    }

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

    public static boolean isHBasePutPefSupport(ObTableClient tableClient, boolean enablePutOptimization) {
        // If client-side optimization is disabled, return false directly
        if (!enablePutOptimization) {
            return false;
        }
        
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

    public static boolean isAllPut(OHOperationType opType, List<? extends Row> actions) {
        if (opType.getValue() == OHOperationType.PUT.getValue()
            || opType.getValue() == OHOperationType.PUT_LIST.getValue()) {
            return true;
        } else {
            for (Row action : actions) {
                if (!(action instanceof Put)) {
                    return false;
                }
            }
            return true;
        }
    }

    public static <T extends Cell> void sortHBaseResult(List<T> cells) {
        cells.sort(HBASE_CELL_COMPARATOR);
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

    public static boolean needTabletId(ObTableClient tableClient) {
        if (tableClient.isOdpMode()) {
            return ObGlobal.isDistributeNeedTabletIdSupport()
                   && ObGlobal.OB_PROXY_VERSION >= ObGlobal.OB_PROXY_VERSION_4_3_6_0
                   && tableClient.getServerCapacity().isSupportDistributedExecute();
        } else {
            return ObGlobal.isDistributeNeedTabletIdSupport()
                   && tableClient.getServerCapacity().isSupportDistributedExecute();
        }
    }

    // names concatenated by periods
    public static String metricsNameBuilder(String... name) {
        StringBuilder builder = new StringBuilder();
        if (name != null) {
            for (String n : name) {
                if (n != null && !n.isEmpty()) {
                    if (builder.length() > 0) {
                        builder.append('.');
                    }
                    builder.append(n);
                }
            }
        }
        return builder.toString();
    }
}
