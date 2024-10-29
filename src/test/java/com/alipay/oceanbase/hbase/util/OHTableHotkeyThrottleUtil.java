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

import com.alipay.oceanbase.hbase.OHTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import org.apache.hadoop.hbase.client.*;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.*;

public class OHTableHotkeyThrottleUtil extends Thread {
    public enum TestType {
        random, specifiedKey
    }

    public enum OperationType {
        put, get, scan
    }

    protected HTableInterface   hTable;
    TestType                    testType;
    OperationType               operationType;

    int                         threadIdx;
    int                         testNum;
    long                        startTime           = 0;
    String                      tableName           = null;

    public int                  threadNum;
    public static List<Integer> unitOperationTimes  = null;
    public static List<Integer> unitBlockTimes      = null;
    public static List<Integer> totalOperationTimes = null;
    public static List<Integer> totalBlockTimes     = null;

    String                      key;                                   // like key-1001
    String                      column              = "Column";
    String                      value               = "value";
    String                      family              = "familyThrottle";
    int                         throttleNum         = 0;
    int                         passNum             = 0;
    int                         unitBlockTime       = 0;
    int                         unitOperationTime   = 0;

    public void init(int threadNum, int threadIdx, long startTime, int testNum, TestType testType,
                     OperationType operationType, String tableName, String family, String key,
                     String column, String value) throws Exception {
        this.threadNum = threadNum;
        this.threadIdx = threadIdx;
        this.startTime = startTime;
        this.testNum = testNum;
        this.testType = testType;
        this.operationType = operationType;
        this.tableName = tableName;
        this.family = family;
        this.column = column;
        this.value = value;

        switch (testType) {
            case random: {
                this.key = null;
                break;
            }
            case specifiedKey: {
                if (key != null) {
                    // only the first key count
                    this.key = key;
                } else {
                    throw new IllegalArgumentException("invalid row key pass into init");
                }
                break;
            }
            default:
                throw new IllegalArgumentException("invalid test type pass into init");
        }

        if (null == unitOperationTimes || threadIdx == 0) {
            unitOperationTimes = new ArrayList<Integer>(threadNum);
            for (int i = 0; i < threadNum; ++i) {
                unitOperationTimes.add(0);
            }
        }
        if (null == unitBlockTimes || threadIdx == 0) {
            unitBlockTimes = new ArrayList<Integer>(threadNum);
            for (int i = 0; i < threadNum; ++i) {
                unitBlockTimes.add(0);
            }
        }
        if (null == totalOperationTimes || threadIdx == 0) {
            totalOperationTimes = new ArrayList<Integer>(threadNum);
            for (int i = 0; i < threadNum; ++i) {
                totalOperationTimes.add(0);
            }
        }
        if (null == totalBlockTimes || threadIdx == 0) {
            totalBlockTimes = new ArrayList<Integer>(threadNum);
            for (int i = 0; i < threadNum; ++i) {
                totalBlockTimes.add(0);
            }
        }

        hTable = ObHTableTestUtil.newOHTableClient(this.tableName);
        ((OHTableClient) hTable).init();
    }

    @Override
    public void run() {
        try {
            switch (testType) {
                case random:
                    runRandom();
                    break;
                case specifiedKey:
                    runSpecifiedKey();
                    break;
                default:
                    System.out.println(Thread.currentThread().getName() + "(idx:" + threadIdx + ")"
                                       + " has no test type to run");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void runRandom() throws Exception {
        System.out.println(Thread.currentThread().getName() + "(idx:" + threadIdx + ")"
                           + " begin to run random " + operationType + " test");
        for (int i = 0; i < testNum; ++i) {
            long randomNum = (long) (Math.random() * 500000);
            String key = "key-" + randomNum;
            switch (operationType) {
                case put:
                    putTest(key);
                    break;
                case get:
                    getTest(key);
                    break;
                case scan:
                    scanTest(key);
                    break;
            }
            // record operation time for each 2s
            if (System.currentTimeMillis() - startTime > 2000) {
                unitOperationTimes.set(threadIdx, unitOperationTime);
                unitBlockTimes.set(threadIdx, unitBlockTime);
                unitOperationTime = 0;
                unitBlockTime = 0;
                while (System.currentTimeMillis() - startTime > 2000)
                    startTime += 2000;
            }
        }
        unitBlockTimes.set(threadIdx, 0);
        unitOperationTimes.set(threadIdx, 0);
        totalOperationTimes.set(threadIdx, throttleNum + passNum);
        totalBlockTimes.set(threadIdx, throttleNum);
    }

    private void runSpecifiedKey() throws Exception {
        System.out.println(Thread.currentThread().getName() + "(idx:" + threadIdx + ")"
                           + " begin to run specified key " + this.key + ", type " + operationType
                           + " test");
        for (int i = 0; i < testNum; ++i) {
            switch (operationType) {
                case put:
                    putTest(this.key);
                    break;
                case get:
                    getTest(this.key);
                    break;
                case scan:
                    scanTest(this.key);
                    break;

            }
            // record operation time for each 2s
            if (System.currentTimeMillis() - startTime > 2000) {
                unitOperationTimes.set(threadIdx, unitOperationTime);
                unitBlockTimes.set(threadIdx, unitBlockTime);
                unitOperationTime = 0;
                unitBlockTime = 0;
                while (System.currentTimeMillis() - startTime > 2000)
                    startTime += 2000;
            }
        }
        unitBlockTimes.set(threadIdx, 0);
        unitOperationTimes.set(threadIdx, 0);
        totalOperationTimes.set(threadIdx, throttleNum + passNum);
        totalBlockTimes.set(threadIdx, throttleNum);
    }

    private void putTest(String key) throws Exception {
        try {
            ++unitOperationTime;
            Put put = new Put(toBytes(key));
            long timestamp = System.currentTimeMillis();
            put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
            hTable.put(put);
            ++passNum;
        } catch (Exception e) {
            if (e.getCause() instanceof ObTableUnexpectedException) {
                if (((ObTableUnexpectedException) e.getCause()).getErrorCode() == -4039) {
                    ++throttleNum;
                    ++unitBlockTime;
                } else {
                    e.printStackTrace();
                    Assert.assertNull(e);
                }
            } else {
                e.printStackTrace();
                assertNull(e);
            }
        }
    }

    private void getTest(String key) throws Exception {
        try {
            ++unitOperationTime;
            Get get = new Get(toBytes(key));
            get.addColumn(family.getBytes(), toBytes(column));
            hTable.get(get);
            ++passNum;
        } catch (Exception e) {
            if (e.getCause() instanceof ObTableUnexpectedException) {
                if (((ObTableUnexpectedException) e.getCause()).getErrorCode() == -4039) {
                    ++throttleNum;
                    ++unitBlockTime;
                } else {
                    e.printStackTrace();
                    assertNull(e);
                }
            } else {
                e.printStackTrace();
                assertNull(e);
            }
        }
    }

    private void scanTest(String key) throws Exception {
        try {
            ++unitOperationTime;
            Scan scan = new Scan();
            scan.addColumn(family.getBytes(), column.getBytes());
            scan.setStartRow(toBytes(key));
            scan.setStopRow(toBytes(key));
            scan.setMaxVersions(9);
            hTable.getScanner(scan);
            ++passNum;
        } catch (Exception e) {
            if (e.getCause() instanceof ObTableUnexpectedException) {
                if (((ObTableUnexpectedException) e.getCause()).getErrorCode() == -4039) {
                    ++throttleNum;
                    ++unitBlockTime;
                } else {
                    e.printStackTrace();
                    assertNull(e);
                }
            } else {
                e.printStackTrace();
                assertNull(e);
            }
        }
    }

    public List<Integer> getUnitOperationTimes() {
        return unitOperationTimes;
    }

    public List<Integer> getUnitBlockTimes() {
        return unitBlockTimes;
    }

    public List<Integer> getTotalOperationTimes() {
        return totalOperationTimes;
    }

    public List<Integer> getTotalBlockTimes() {
        return totalBlockTimes;
    }
}
