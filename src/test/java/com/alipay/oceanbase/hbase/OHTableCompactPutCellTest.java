/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2022 OceanBase Group
 * %%
 * OBKV HBase Client Framework is licensed under Mulan PSL v2.
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

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.OHOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObHbaseRequest;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class OHTableCompactPutCellTest {
    private ExecutorService executorService;

    @Before
    public void setUp() {
        executorService = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() {
        executorService.shutdownNow();
    }

    @Test
    public void testPutAlwaysUsesCompactCellsWithoutTtl() throws Exception {
        Put put = new Put(Bytes.toBytes("row"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q1"), 1001L, Bytes.toBytes("value1"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qualifier-2"), 1002L,
            Bytes.toBytes("value-2"));

        ObHbaseRequest request = newTable().buildHbaseRequest(Collections.singletonList(put),
            OHOperationType.PUT);

        assertEquals(1, request.getCfRows().size());
        assertTrue(request.getCfRows().get(0).hasCompactCells());
        assertTrue(request.encode().length > 0);
    }

    @Test
    public void testPutAlwaysUsesCompactCellsWithTtlAndMultipleRows() throws Exception {
        Put first = new Put(Bytes.toBytes("row-1"));
        first.setTTL(60000L);
        first.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q1"), 2001L, Bytes.toBytes("value-1"));
        first.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q2"), 2002L, Bytes.toBytes("value-2"));
        first.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("q3"), 2003L, Bytes.toBytes("value-3"));
        Put second = new Put(Bytes.toBytes("row-2"));
        second.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q4"), 3001L, Bytes.toBytes("value-4"));

        ObHbaseRequest compact = newTable().buildHbaseRequest(Arrays.asList(first, second),
            OHOperationType.PUT_LIST);

        assertEquals(2, compact.getCfRows().size());
        for (int i = 0; i < compact.getCfRows().size(); i++) {
            assertTrue(compact.getCfRows().get(i).hasCompactCells());
        }
        assertTrue(compact.encode().length > 0);
    }

    @Test
    public void testBufferedCompactRequestOwnsQualifierAndValueBytes() throws Exception {
        Put put = new Put(Bytes.toBytes("row"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qualifier"), 4001L,
            Bytes.toBytes("original-value"));
        Cell source = put.getFamilyCellMap().get(Bytes.toBytes("cf")).get(0);

        OHTable compactTable = newTable();
        compactTable.setAutoFlush(false);
        ObHbaseRequest compact = compactTable.buildHbaseRequest(Collections.singletonList(put),
            OHOperationType.PUT);
        byte[] encodedBeforeMutation = compact.encode();

        Arrays.fill(source.getQualifierArray(), source.getQualifierOffset(),
            source.getQualifierOffset() + source.getQualifierLength(), (byte) 'x');
        Arrays.fill(source.getValueArray(), source.getValueOffset(), source.getValueOffset()
                                                                     + source.getValueLength(),
            (byte) 'y');

        assertArrayEquals(encodedBeforeMutation, compact.encode());
    }

    private OHTable newTable() {
        return new OHTable(Bytes.toBytes("test"), mock(ObTableClient.class), executorService);
    }
}
