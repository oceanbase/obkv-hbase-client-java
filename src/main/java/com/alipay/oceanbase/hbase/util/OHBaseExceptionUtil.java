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
                return (TableNotFoundException) new TableNotFoundException(e.getMessage()).initCause(e);
            } else if (errCode == ResultCodes.OB_KV_HBASE_TABLE_EXISTS.errorCode) {
                return (TableExistsException) new TableExistsException(e.getMessage()).initCause(e);
            } else if (errCode == ResultCodes.OB_KV_HBASE_NAMESPACE_NOT_FOUND.errorCode) {
                return (NamespaceNotFoundException) new NamespaceNotFoundException(e.getMessage()).initCause(e);
            } else if (errCode == ResultCodes.OB_KV_TABLE_NOT_ENABLED.errorCode) {
                return (TableNotEnabledException) new TableNotEnabledException(e.getMessage()).initCause(e);
            } else if (errCode == ResultCodes.OB_KV_TABLE_NOT_DISABLED.errorCode) {
                return (TableNotDisabledException) new TableNotDisabledException(e.getMessage()).initCause(e);
            }
        }
        return new IOException("Failed to execute request", e);
    }
}
