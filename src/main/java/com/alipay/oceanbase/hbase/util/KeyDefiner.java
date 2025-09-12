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

import com.alipay.oceanbase.hbase.constants.OHConstants;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class KeyDefiner {
    public static String genPooledOHTableAttributeName(String tableName, String key) {
        return tableName + OHConstants.HBASE_HTABLE_POOL_SEPERATOR + key;
    }
    public static String[] parsePoolOHTableAttributeName(String key) {
        if (key == null) {
            return null;
        }
        String[] parsedKey = key.split("\\" + OHConstants.HBASE_HTABLE_POOL_SEPERATOR);
        if (parsedKey.length != 2) {
            return null;
        }
        return parsedKey;
    }
}
