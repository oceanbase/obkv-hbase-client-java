package com.alipay.oceanbase.hbase.util;

import static com.alipay.oceanbase.rpc.ObGlobal.OB_VERSION;
import static com.alipay.oceanbase.rpc.ObGlobal.calcVersion;


public class FeatureSupport {
    // check empty family get/scan supported
    public static boolean isEmptyFamilySupported() {
        return (OB_VERSION >= calcVersion(4, 2, 3, 0)
                && OB_VERSION < calcVersion(4, 3, 0, 0))
                || (OB_VERSION > calcVersion(4, 3, 4, 0));
    }
}
