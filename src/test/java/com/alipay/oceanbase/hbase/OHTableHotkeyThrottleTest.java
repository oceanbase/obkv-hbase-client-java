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
    public int  threadNum    = 64;
    public int  repeatKeyNum = 4;
    public long startTime;

    @Test
    public void testThrottle() throws Exception {
        try {
            List<OHTableHotkeyThrottleUtil> allWorkers = new ArrayList<>();
            startTime = System.currentTimeMillis();
            for (int i = 0; i < threadNum; ++i) {
                OHTableHotkeyThrottleUtil worker = new OHTableHotkeyThrottleUtil();
                if (i < threadNum/4) {
                    worker.init(threadNum, i, startTime, 500, OHTableHotkeyThrottleUtil.TestType.random, OHTableHotkeyThrottleUtil.OperationType.put,
                            "test", "familyThrottle", null, "Column", "value");
                } else if (i < threadNum/3) {
                    worker.init(threadNum, i, startTime, 500, OHTableHotkeyThrottleUtil.TestType.random, OHTableHotkeyThrottleUtil.OperationType.get,
                            "test", "familyThrottle", null, "Column", "value");
                } else if (i < threadNum/2) {
                    worker.init(threadNum, i, startTime, 500, OHTableHotkeyThrottleUtil.TestType.random, OHTableHotkeyThrottleUtil.OperationType.scan,
                            "test", "familyThrottle", null, "Column", "value");
                } else {
                    int rowKeyNum = i % repeatKeyNum;
                    String key = "key-" + rowKeyNum;
                    worker.init(threadNum, i, startTime, 500, OHTableHotkeyThrottleUtil.TestType.specifiedKey, OHTableHotkeyThrottleUtil.OperationType.put,
                            "test", "familyThrottle", key, "Column", "value");
                }
                allWorkers.add(worker);
                worker.start();
            }
            // for each 2s, get unitOperationTimes and unitOperationTimes from ObTableHotkeyThrottleUtil and print
            for (int i = 0; i < 20; ++i) {
                Thread.sleep(2000);
                List<Integer> blockList = allWorkers.get(0).getUnitBlockTimes();
                List<Integer> operationList = allWorkers.get(0).getUnitOperationTimes();
                System.out.print("Block times: ");
                for (int j = 0; j < threadNum; ++j) {
                    System.out.print(blockList.get(j) + " ");
                }
                System.out.print("\nOperation times: ");
                for (int j = 0; j < threadNum; ++j) {
                    System.out.print(operationList.get(j) + " ");
                }
                System.out.print("\n");
            }

            for (int i = 0; i < threadNum; ++i) {
                allWorkers.get(i).join();
                System.out.println("Thread " + i + "has finished");
            }

            // get totalBlockTimes and totalOperationTimes from ObTableHotkeyThrottleUtil and print
            List<Integer> blockList = allWorkers.get(0).getTotalBlockTimes();
            List<Integer> operationList = allWorkers.get(0).getTotalOperationTimes();
            System.out.print("Total Block times: ");
            for (int j = 0; j < threadNum; ++j) {
                System.out.print(blockList.get(j) + " ");
            }
            System.out.print("\nTotal Operation times: ");
            for (int j = 0; j < threadNum; ++j) {
                System.out.print(operationList.get(j) + " ");
            }
            System.out.print("\n");
        } catch (Exception e) {
            e.printStackTrace();
            assertNull(e);
        }
    }
}
