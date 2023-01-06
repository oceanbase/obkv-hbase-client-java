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
import com.alipay.oceanbase.hbase.ObHTableTestUtil;
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import org.apache.hadoop.hbase.client.*;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.*;

public class OHTableHotkeyThrottleUtil extends Thread {
    public enum TestType {
        random, specifiedKey
    }
    public enum OperationType {
        put, get, scan
    }
    protected HTableInterface hTable;
    TestType testType;
    OperationType operationType;

    int testNum = 500;
    int throttleNum = 0;
    int passNum = 0;
    String key; // like key-1001
    String column = "Column";
    String value = "value";
    String family = "familyThrottle";

    public void init(TestType testType, OperationType operationType, String... key) throws Exception {
        this.testType = testType;
        this.operationType = operationType;

        switch (testType) {
            case random: {
                this.key = null;
                break;
            }
            case specifiedKey: {
                if (key != null && key.length > 0) {
                    // only the first key count
                    this.key = key[0];
                } else {
                    throw new IllegalArgumentException("invalid row key pass into init");
                }
                break;
            }
            default:
                throw new IllegalArgumentException("invalid test type pass into init");
        }

        hTable = ObHTableTestUtil.newOHTableClient("test");
        ((OHTableClient) hTable).init();
    }

    @Override
    public void run() {
        try{
            switch (testType) {
                case random:
                    runRandom();
                    break;
                case specifiedKey:
                    runSpecifiedKey();
                    break;
                default:
                    System.out.println(Thread.currentThread().getName() + " has no test type to run");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void runRandom() throws Exception {
        System.out.println(Thread.currentThread().getName() + " begin to run random " + operationType + " test");
        for (int i = 0; i < testNum; ++i) {
            long randomNum = (long)(Math.random() * 500000);
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
        }
    }

    private void runSpecifiedKey() throws Exception {
        System.out.println(Thread.currentThread().getName() + " begin to run specified key " + this.key
                + ", type " + operationType + " test");
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
        }
    }

    private void putTest(String key) throws Exception {

        try {
            Put put = new Put(toBytes(key));
            long timestamp = System.currentTimeMillis();
            put.add(family.getBytes(), column.getBytes(), timestamp, toBytes(value));
            hTable.put(put);
            ++passNum;
        } catch (Exception e) {
            if (e.getCause() instanceof ObTableUnexpectedException) {
                if (((ObTableUnexpectedException) e.getCause()).getErrorCode() == -4039) {
                    if (++throttleNum % 50 == 0) {
                        System.out.println(Thread.currentThread().getName() + " rowkey is " + key + " has pass "
                                + passNum + " operations, and has throttle " + throttleNum + " operations --putTest");
                    }
                } else {
                    e.printStackTrace();
                }
            } else {
                e.printStackTrace();
                assertNull(e);
            }
        }
    }

    private void getTest(String key) throws Exception {
        try {
            Get get = new Get(toBytes(key));
            get.addColumn(family.getBytes(), toBytes(column));
            hTable.get(get);
            ++passNum;
        } catch (Exception e) {
            if (e.getCause() instanceof ObTableUnexpectedException) {
                if (((ObTableUnexpectedException) e.getCause()).getErrorCode() == -4039) {
                    if (++throttleNum % 50 == 0) {
                        System.out.println(Thread.currentThread().getName() + " rowkey is " + key + " has pass "
                                + passNum + " operations, and has throttle " + throttleNum + " operations --getTest");
                    }
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
                    if (++throttleNum % 50 == 0) {
                        System.out.println(Thread.currentThread().getName() + " rowkey is " + key + " has pass "
                                + passNum + " operations, and has throttle " + throttleNum + " operations --scanTest");
                    }
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
}


