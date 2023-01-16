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

package com.alipay.oceanbase.hbase;

import com.alipay.oceanbase.hbase.util.OHTableHotkeyThrottleUtil;
import org.junit.Test;

import java.util.List;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class OHTableHotkeyThrottleTest {
    public int threadNum    = 64;
    public int repeatKeyNum = 4;

    @Test
    public void testThrottle() throws Exception {
        try {
            List<OHTableHotkeyThrottleUtil> allWorkers = new ArrayList<>();
            for (int i = 0; i < threadNum; ++i) {
                OHTableHotkeyThrottleUtil worker = new OHTableHotkeyThrottleUtil();
                if (i < threadNum/4) {
                    worker.init(OHTableHotkeyThrottleUtil.TestType.random, OHTableHotkeyThrottleUtil.OperationType.put);
                } else if (i < threadNum/3) {
                    worker.init(OHTableHotkeyThrottleUtil.TestType.random, OHTableHotkeyThrottleUtil.OperationType.get);
                } else if (i < threadNum/2) {
                    worker.init(OHTableHotkeyThrottleUtil.TestType.random, OHTableHotkeyThrottleUtil.OperationType.scan);
                } else {
                    int rowKeyNum = i % repeatKeyNum;
                    String key = "key-" + rowKeyNum;
                    worker.init(OHTableHotkeyThrottleUtil.TestType.specifiedKey, OHTableHotkeyThrottleUtil.OperationType.get, key);
                }
                allWorkers.add(worker);
                worker.start();
            }
            for (int i = 0; i < threadNum; ++i) {
                allWorkers.get(i).join();
                System.out.println("Thread " + i + " has finished");
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertNull(e);
        }
    }
}
