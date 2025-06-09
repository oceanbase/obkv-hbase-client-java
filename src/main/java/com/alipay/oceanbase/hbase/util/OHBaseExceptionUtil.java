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

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import org.apache.hadoop.hbase.*;

import java.io.IOException;

public class OHBaseExceptionUtil {
    public static IOException convertTableException(Exception e) {
        if (e instanceof ObTableException) {
            final int errCode = ((ObTableException) e).getErrorCode();
            if (errCode == ResultCodes.OB_KV_HBASE_TABLE_NOT_EXISTS.errorCode) {
                return (TableNotFoundException) new TableNotFoundException(e.getMessage())
                    .initCause(e);
            } else if (errCode == ResultCodes.OB_KV_HBASE_TABLE_EXISTS.errorCode) {
                return (TableExistsException) new TableExistsException(e.getMessage()).initCause(e);
            } else if (errCode == ResultCodes.OB_KV_HBASE_NAMESPACE_NOT_FOUND.errorCode) {
                return (NamespaceNotFoundException) new NamespaceNotFoundException(e.getMessage())
                    .initCause(e);
            } else if (errCode == ResultCodes.OB_KV_TABLE_NOT_ENABLED.errorCode) {
                return (TableNotEnabledException) new TableNotEnabledException(e.getMessage())
                    .initCause(e);
            } else if (errCode == ResultCodes.OB_KV_TABLE_NOT_DISABLED.errorCode) {
                return (TableNotDisabledException) new TableNotDisabledException(e.getMessage())
                    .initCause(e);
            }
        }
        return new IOException("Failed to execute request", e);
    }
}
