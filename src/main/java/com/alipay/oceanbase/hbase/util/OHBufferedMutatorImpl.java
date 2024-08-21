/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2024 OceanBase Group
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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Mutation;

import java.io.IOException;
import java.util.List;

@InterfaceAudience.Private
public class OHBufferedMutatorImpl implements BufferedMutator {
    public OHBufferedMutatorImpl(OHConnectionImpl ohConnection, BufferedMutatorParams params) {
    }

    @Override
    public TableName getName() {
        return null;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

    @Override
    public void mutate(Mutation mutation) throws IOException {

    }

    @Override
    public void mutate(List<? extends Mutation> list) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public long getWriteBufferSize() {
        return 0;
    }
}
