package com.alipay.oceanbase.hbase;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;

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

/// Only support odp mode for now.
public class OHVarPrefixPartitionTest extends HTableTestBase {
    @Before
    public void before() throws Exception {
        hTable = ObHTableTestUtil.newOHTableClient("test_var_prefix_partition");
        ((OHTableClient) hTable).init();
    }

    @After
    public void finish() throws IOException {
        hTable.close();
    }

    @After
    public void after() throws IOException {
        hTable.close();
    }
}
