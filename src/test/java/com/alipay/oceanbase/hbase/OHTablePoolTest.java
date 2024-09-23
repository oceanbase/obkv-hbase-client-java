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

import com.alipay.remoting.util.ConcurrentHashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.PoolMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static com.alipay.oceanbase.hbase.util.ObTableClientManager.OB_TABLE_CLIENT_INSTANCE;

public class OHTablePoolTest extends HTableTestBase {
    protected OHTablePool ohTablePool;

    private OHTablePool newOHTablePool(final int maxSize, final PoolMap.PoolType poolType) {
        OHTablePool pool = new OHTablePool(new Configuration(), maxSize, poolType);
        pool.setFullUserName("test", ObHTableTestUtil.FULL_USER_NAME);
        pool.setPassword("test", ObHTableTestUtil.PASSWORD);
        if (ObHTableTestUtil.ODP_MODE) {
            pool.setOdpAddr("test", ObHTableTestUtil.ODP_ADDR);
            pool.setOdpPort("test", ObHTableTestUtil.ODP_PORT);
            pool.setOdpMode("test", ObHTableTestUtil.ODP_MODE);
            pool.setDatabase("test", ObHTableTestUtil.DATABASE);
        } else {
            pool.setParamUrl("test", ObHTableTestUtil.PARAM_URL);
            pool.setSysUserName("test", ObHTableTestUtil.SYS_USER_NAME);
            pool.setSysPassword("test", ObHTableTestUtil.SYS_PASSWORD);
        }
        return pool;
    }

    @Before
    public void setup() throws IOException {
        Configuration c = new Configuration();
        ohTablePool = newOHTablePool(10, null);
        ohTablePool.setRuntimeBatchExecutor("test", Executors.newFixedThreadPool(3));
        hTable = ohTablePool.getTable("test");
        //        hTable = ohTablePool.getTable("n1:test");
    }

    @After
    public void finish() throws IOException {
        hTable.close();
    }

    public void test_current_get_close(final OHTablePool ohTablePool, int concurrency, int maxSize) {
        final CountDownLatch pre = new CountDownLatch(concurrency);
        final CountDownLatch suf = new CountDownLatch(concurrency);
        final ConcurrentHashSet<HTableInterface> ohTableSet = new ConcurrentHashSet<HTableInterface>();
        final ConcurrentHashSet<HTableInterface> pooledHTableSet = new ConcurrentHashSet<HTableInterface>();
        for (int i = 0; i < concurrency; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    pre.countDown();
                    try {
                        pre.await();
                    } catch (InterruptedException e) {
                        //
                    }
                    OHTablePool.PooledOHTable pooledOHTable = ((OHTablePool.PooledOHTable) ohTablePool
                        .getTable("test"));
                    //                    OHTablePool.PooledOHTable pooledOHTable = ((OHTablePool.PooledOHTable) ohTablePool
                    //                        .getTable("n1:test"));
                    HTableInterface htable = pooledOHTable.getTable();
                    ohTableSet.add(htable);
                    pooledHTableSet.add(pooledOHTable);
                    suf.countDown();
                }
            }).start();
        }

        try {
            suf.await();
        } catch (InterruptedException e) {
            //
        }

        for (HTableInterface htable : pooledHTableSet) {
            try {
                htable.close();
            } catch (IOException e) {
                Assert.fail();
            }
        }
        Assert.assertEquals(concurrency, ohTableSet.size());
        Assert.assertEquals(1, OB_TABLE_CLIENT_INSTANCE.size());
        Assert.assertEquals(maxSize, ohTablePool.getCurrentPoolSize("test"));
        //        Assert.assertEquals(maxSize, ohTablePool.getCurrentPoolSize("n1:test"));
    }

    @Test
    public void test_refresh_table_entry() throws Exception {
        ohTablePool.refreshTableEntry("test", "family1", false);
        ohTablePool.refreshTableEntry("test", "family1", true);
        //        ohTablePool.refreshTableEntry("n1:test", "family1", false);
        //        ohTablePool.refreshTableEntry("n1:test", "family1", true);
    }

    @Test
    public void test_all_type_pool() throws Exception {
        OHTablePool ohTablePool = newOHTablePool(10, PoolMap.PoolType.Reusable);

        // test first
        test_current_get_close(ohTablePool, 1000, 10);
        // test reuse
        test_current_get_close(ohTablePool, 1000, 10);
        ohTablePool.close();

        ohTablePool = newOHTablePool(10, PoolMap.PoolType.ThreadLocal);
        //ohTablePool.load("test", "oceanbase_stable_test_host");

        // test first
        test_current_get_close(ohTablePool, 1000, 1);
        // test reuse
        test_current_get_close(ohTablePool, 1000, 1);
        ohTablePool.close();

        ohTablePool = newOHTablePool(10, PoolMap.PoolType.RoundRobin);
        //ohTablePool.load("test", "oceanbase_stable_test_host");

        // test first
        test_current_get_close(ohTablePool, 1000, 10);
        // test reuse
        test_current_get_close(ohTablePool, 1000, 10);
        ohTablePool.close();
    }

}
