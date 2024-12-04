package com.alipay.oceanbase.hbase.util;

import com.alipay.oceanbase.rpc.ObGlobal;

public class CompatibilityUtil {
    public static boolean isBatchSupport() {
        return ObGlobal.OB_VERSION > ObGlobal.OB_VERSION_4_3_4_0;
    }
}
