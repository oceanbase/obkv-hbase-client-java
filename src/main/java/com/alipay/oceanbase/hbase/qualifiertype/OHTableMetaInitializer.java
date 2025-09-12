/*-
 * #%L
 * OceanBase Table Hbase Framework
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
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

package com.alipay.oceanbase.hbase.qualifiertype;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author qiaoyunyao
 * @version 1: OHTableMetaInitializer.java, v 0.1 2023年07月14日 16:59 qiaoyunyao Exp $
 */
public class OHTableMetaInitializer {

    private static ConcurrentHashMap<String, Map<String, ObQualifierType>> tableFamilyTypeMap = new ConcurrentHashMap<>();

    // 获取用户qualifier的类型信息，缓存到qualifierTypeMap中。新插入的k-v会覆盖原本的v
    public static void setQualifierType(String tableName, String family, Map<String, ObQualifierType> qualifierTypeMaps) {
        String tableFamily = tableName + "|" + family;
        tableFamilyTypeMap.put(tableFamily, qualifierTypeMaps);

    }

    public static ConcurrentHashMap<String, Map<String, ObQualifierType>> getTableFamilyTypeMap() {
        return tableFamilyTypeMap;
    }

    public static void clearQualifierMap(){
        tableFamilyTypeMap.clear();
    }

}
