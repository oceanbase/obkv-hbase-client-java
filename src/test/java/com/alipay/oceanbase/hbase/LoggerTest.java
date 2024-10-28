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

import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest(OHTable.class)
@PowerMockIgnore({ "javax.crypto.*" })
public class LoggerTest {
    HTableInterface hTableMock;

    @BeforeClass
    public void setup() throws IOException {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set("rs.list.acquire.read.timeout", "10000");
        hTableMock = new OHTable(c, "test".getBytes());
    }

    /**
     * mock 掉 OHTable 里的 obTableClient，使其链接 ob 失败
     * @throws Exception
     */
    @Test
    public void test() throws Exception {
        MemberModifier.field(OHTable.class, "obTableClient").set(hTableMock, null);
        try {
            Scan scan = new Scan();
            scan.addColumn("family1".getBytes(), "c1".getBytes());
            scan.setStartRow(toBytes("key" + "_" + 0));
            scan.setStopRow(toBytes("key" + "_" + 9));
            hTableMock.getScanner(scan);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            Append append = new Append("key".getBytes());
            append.add("family1".getBytes(), "c1".getBytes(), toBytes("_append"));
            hTableMock.append(append);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            Increment increment = new Increment("key".getBytes());
            increment.addColumn("family1".getBytes(), "c1".getBytes(), 1);
            hTableMock.increment(increment);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            hTableMock.incrementColumnValue("key".getBytes(), "family1".getBytes(),
                "c1".getBytes(), 1);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }
    }
}
