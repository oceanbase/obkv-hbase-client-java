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
import com.alipay.oceanbase.rpc.exception.FeatureNotSupportedException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import java.util.Arrays;
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
        int qualifierLen = qualifier.length - familyLen - 1;
        if (qualifierLen <= 0) {
            throw new RuntimeException("Cannot get qualifier name");
        }
        byte[] newQualifier = Arrays.copyOfRange(qualifier, familyLen + 1, qualifier.length);
        return new byte[][] { family, newQualifier };
    }

    public static boolean isHBasePutPefSupport(ObTableClient tableClient) {
        if (tableClient.isOdpMode()) {
            throw new FeatureNotSupportedException("not supported yet");
        } else {
            // server version support and distributed capacity is enabled
            return ObGlobal.isHBasePutPerfSupport() && tableClient.getServerCapacity().isSupportDistributedExecute();
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
}
