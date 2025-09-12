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

/**
 * @author qiaoyunyao
 * @version 1: ObQualifierType.java, v 0.1 2023年07月12日 18:16 qiaoyunyao Exp $
 */
public enum ObQualifierType {
    INVALID_TYPE(0), Long(1), Int(2), Double(3);

    private final int code;

    ObQualifierType(int code) {
        this.code = code;
    }

    @Override
    public String toString() {
        switch (code) {
            case 1:
                return "Long";
            case 2:
                return "Int";
            case 3:
                return "Double";
            default:
                return "INVALID_TYPE";
        }
    }
}