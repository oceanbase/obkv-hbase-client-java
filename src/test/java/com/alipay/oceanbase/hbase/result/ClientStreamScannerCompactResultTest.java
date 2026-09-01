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

package com.alipay.oceanbase.hbase.result;

import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHBaseCellBatch;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHBaseCellRow;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryAsyncStreamResult;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClientStreamScannerCompactResultTest {

    @Test
    public void testCompactResultUsesKeyValueAndDirectConsume() throws Exception {
        ObTableClientQueryAsyncStreamResult streamResult = compactStreamResult(compactRow(
            new String[] { "q-2", "q-1" }, new long[] { 101L, 102L }));
        ClientStreamScanner scanner = new ClientStreamScanner(streamResult, "test", bytes("f"),
            false, null);

        Result result = scanner.next();

        assertEquals(2, result.size());
        assertTrue(result.rawCells()[0] instanceof KeyValue);
        assertArrayEquals(bytes("q-1"), CellUtil.cloneQualifier(result.rawCells()[0]));
        assertArrayEquals(bytes("q-2"), CellUtil.cloneQualifier(result.rawCells()[1]));
        verify(streamResult, never()).getRow();
        verify(streamResult, never()).getCacheRows();
    }

    @Test
    public void testCompactTableGroupResultUsesQualifierOffsets() throws Exception {
        ObTableClientQueryAsyncStreamResult streamResult = compactStreamResult(compactRow(
            new String[] { "f1\0q-1", "f2\0q-2" }, new long[] { 102L, 101L }));
        ClientStreamScanner scanner = new ClientStreamScanner(streamResult, "test", new byte[0],
            true, null);

        Result result = scanner.next();

        assertEquals(2, result.size());
        assertTrue(result.rawCells()[0] instanceof KeyValue);
        assertArrayEquals(bytes("f1"), CellUtil.cloneFamily(result.rawCells()[0]));
        assertArrayEquals(bytes("q-1"), CellUtil.cloneQualifier(result.rawCells()[0]));
        assertArrayEquals(bytes("f2"), CellUtil.cloneFamily(result.rawCells()[1]));
        assertArrayEquals(bytes("q-2"), CellUtil.cloneQualifier(result.rawCells()[1]));
    }

    private static ObTableClientQueryAsyncStreamResult compactStreamResult(ObHBaseCellRow row)
                                                                                              throws Exception {
        ObTableClientQueryAsyncStreamResult streamResult = mock(ObTableClientQueryAsyncStreamResult.class);
        when(streamResult.next()).thenReturn(true);
        when(streamResult.isCurrentHBaseCell()).thenReturn(true);
        when(streamResult.drainCurrentHBaseRow()).thenReturn(row);
        when(streamResult.getTableName()).thenReturn("test");
        return streamResult;
    }

    private static ObHBaseCellRow compactRow(String[] qualifiers, long[] timestamps)
                                                                                    throws Exception {
        assertEquals(qualifiers.length, timestamps.length);
        Constructor<ObHBaseCellBatch> batchConstructor = ObHBaseCellBatch.class
            .getDeclaredConstructor(int.class);
        batchConstructor.setAccessible(true);
        ObHBaseCellBatch batch = batchConstructor.newInstance(qualifiers.length);
        Method setCell = ObHBaseCellBatch.class.getDeclaredMethod("setCell", int.class,
            byte[].class, byte[].class, long.class, byte[].class);
        setCell.setAccessible(true);
        byte[] rowKey = bytes("row-1");
        for (int i = 0; i < qualifiers.length; i++) {
            setCell.invoke(batch, i, rowKey, bytes(qualifiers[i]), timestamps[i], bytes("value-"
                                                                                        + i));
        }

        Constructor<ObHBaseCellRow> rowConstructor = ObHBaseCellRow.class
            .getDeclaredConstructor(byte[].class);
        rowConstructor.setAccessible(true);
        ObHBaseCellRow row = rowConstructor.newInstance(rowKey);
        Method addSlice = ObHBaseCellRow.class.getDeclaredMethod("addSlice",
            ObHBaseCellBatch.class, int.class, int.class);
        addSlice.setAccessible(true);
        addSlice.invoke(row, batch, 0, qualifiers.length);
        return row;
    }

    private static byte[] bytes(String value) {
        return Bytes.toBytes(value);
    }
}
