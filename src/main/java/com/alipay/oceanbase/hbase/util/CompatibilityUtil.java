package com.alipay.oceanbase.hbase.util;

import com.alipay.oceanbase.rpc.ObGlobal;

public class CompatibilityUtil {
    public static boolean isBatchSupport() {
        return (ObGlobal.OB_VERSION >= ObGlobal.OB_VERSION_4_2_5_1 && ObGlobal.OB_VERSION < ObGlobal.OB_VERSION_4_3_0_0)
                || ObGlobal.OB_VERSION >= ObGlobal.OB_VERSION_4_3_5_0;
    }
}
