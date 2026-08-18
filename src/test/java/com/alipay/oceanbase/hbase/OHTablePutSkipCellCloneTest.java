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

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.OHOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObHbaseCfRows;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObHbaseRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OHTablePutSkipCellCloneTest {

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
    public void testShareContiguousReturnsByteArray() {
        byte[] q = Bytes.toBytes("qual");
        byte[] v = Bytes.toBytes("val");
        Cell bare = mock(Cell.class);
        when(bare.getQualifierArray()).thenReturn(q);
        when(bare.getQualifierOffset()).thenReturn(0);
        when(bare.getQualifierLength()).thenReturn(q.length);
        when(bare.getValueArray()).thenReturn(v);
        when(bare.getValueOffset()).thenReturn(0);
        when(bare.getValueLength()).thenReturn(v.length);
        Object qObj = OHTable.bytesForPutCell(bare, true, true);
        Object vObj = OHTable.bytesForPutCell(bare, true, false);
        assertTrue(qObj instanceof byte[]);
        assertTrue(vObj instanceof byte[]);
        assertSame(q, qObj);
        assertSame(v, vObj);
    }

    @Test
    public void testShareSliceReturnsObBytesStringView() {
        byte[] qBuf = new byte[16];
        byte[] q = Bytes.toBytes("qual");
        System.arraycopy(q, 0, qBuf, 3, q.length);
        byte[] vBuf = new byte[16];
        byte[] v = Bytes.toBytes("val");
        System.arraycopy(v, 0, vBuf, 2, v.length);
        Cell sliced = new KeyValue(Bytes.toBytes("row"), 0, 3, Bytes.toBytes("cf"), 0, 2, qBuf, 3,
            q.length, System.currentTimeMillis(), KeyValue.Type.Put, vBuf, 2, v.length);
        ObBytesString qView = (ObBytesString) OHTable.bytesForPutCell(sliced, true, true);
        ObBytesString vView = (ObBytesString) OHTable.bytesForPutCell(sliced, true, false);
        assertSame(sliced.getQualifierArray(), qView.bytes);
        assertSame(sliced.getValueArray(), vView.bytes);
        assertEquals(sliced.getQualifierOffset(), qView.offset);
        assertEquals(sliced.getValueOffset(), vView.offset);
        assertEquals(q.length, qView.length());
        assertEquals(v.length, vView.length());
    }

    @Test
    public void testShareDisabledAlwaysClones() {
        byte[] q = Bytes.toBytes("qual");
        byte[] v = Bytes.toBytes("val");
        Cell contiguous = contiguousCell(Bytes.toBytes("row"), Bytes.toBytes("cf"), q, v);
        Object qObj = OHTable.bytesForPutCell(contiguous, false, true);
        Object vObj = OHTable.bytesForPutCell(contiguous, false, false);
        assertTrue(qObj instanceof byte[]);
        assertTrue(vObj instanceof byte[]);
        assertNotSame(q, qObj);
        assertNotSame(v, vObj);
        assertTrue(Bytes.equals(q, (byte[]) qObj));
        assertTrue(Bytes.equals(v, (byte[]) vObj));
    }

    @Test
    public void testBuildHbaseRequestSharesViewForSynchronousPut() throws Exception {
        OHTable table = new OHTable(Bytes.toBytes("t"), mock(ObTableClient.class), executorService);
        byte[] q = Bytes.toBytes("q1");
        byte[] v = Bytes.toBytes("v1");
        Put put = new Put(Bytes.toBytes("row"));
        put.addColumn(Bytes.toBytes("cf"), q, v);
        Cell src = put.getFamilyCellMap().get(Bytes.toBytes("cf")).get(0);

        ObHbaseRequest request = table.buildHbaseRequest(Collections.singletonList(put),
            OHOperationType.PUT);
        ObHbaseCfRows cfRows = request.getCfRows().get(0);
        assertTrue(cfRows.hasCompactCells());
        assertSame(src.getQualifierArray(), compactByteArrays(cfRows, "compactQualifierArrays")[0]);
        assertSame(src.getValueArray(), compactByteArrays(cfRows, "compactValueArrays")[0]);
    }

    @Test
    public void testLegacyTtlReusesSingleValueClone() throws Exception {
        OHTable table = new OHTable(Bytes.toBytes("t"), mock(ObTableClient.class), executorService);
        byte[] value = Bytes.toBytes("payload");
        KeyValue kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("q"),
            value);
        long ttl = 60_000L;
        ObTableOperation op = table.buildObTableOperation(kv, ObTableOperationType.INSERT_OR_UPDATE,
            ttl);
        assertTrue(op != null);
        java.lang.reflect.Method buildMutation = OHTable.class.getDeclaredMethod("buildMutation",
            Cell.class, ObTableOperationType.class, boolean.class, byte[].class, Long.class);
        buildMutation.setAccessible(true);
        Object mutation = buildMutation.invoke(table, kv, ObTableOperationType.INSERT_OR_UPDATE,
            false, null, ttl);
        assertTrue(mutation != null);
    }

    private static Cell contiguousCell(byte[] row, byte[] family, byte[] qualifier, byte[] value) {
        return new KeyValue(row, 0, row.length, family, 0, family.length, qualifier, 0,
            qualifier.length, System.currentTimeMillis(), KeyValue.Type.Put, value, 0, value.length);
    }

    private static byte[][] compactByteArrays(ObHbaseCfRows cfRows, String fieldName)
                                                                                     throws Exception {
        Field field = ObHbaseCfRows.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (byte[][]) field.get(cfRows);
    }
}
