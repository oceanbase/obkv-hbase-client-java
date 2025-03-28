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

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.Arrays;

import static java.lang.String.format;

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

    public static String generateIncrFieldErrMsg(String errMsg) {
        // [-10516][OB_KV_HBASE_INCR_FIELD_IS_NOT_LONG][When invoking the Increment interface, only HBase cells with a length of 8 can be converted to int64_t. the current length of the HBase cell is '4'.][ip:port][trace_id]
        int start = errMsg.indexOf('\'');
        int stop = errMsg.lastIndexOf('\'');
        String errByte = errMsg.substring(start + 1, stop);
        errMsg = format("Field is not a long, it's %s bytes wide", errByte);
        return errMsg;
    }
}
