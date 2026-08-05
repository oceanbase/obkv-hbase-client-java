/*-
 * #%L
 * com.oceanbase:obkv-hbase-client
 * %%
 * Copyright (C) 2022 - 2026 OceanBase Group
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
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationLevel;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjMeta;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableSingleOpEntity;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableSingleOpResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.AbstractQueryStreamResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHBaseCellBatch;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHBaseCellRow;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryResult;
import com.alipay.oceanbase.hbase.result.OHBaseResultCell;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class OHTableGetMaxRowResultTest {

    private OHTable         table;
    private ExecutorService executorService;

    @Before
    public void setUp() {
        executorService = Executors.newSingleThreadExecutor();
        table = new OHTable(Bytes.toBytes("test"), mock(ObTableClient.class), executorService, true);
    }

    @After
    public void tearDown() {
        executorService.shutdownNow();
    }

    @Test
    public void testPointGetValidatesOnlyFirstCellAndUsesExpectedRowKey() throws Exception {
        byte[] expectedRowKey = Bytes.toBytes("row-1");
        AbstractQueryStreamResult streamResult = stream(row("row-1", "q1", 3L, "v1"),
            row("unexpected-later-row", "q2", 2L, "v2"));
        List<Cell> keyValues = new ArrayList<>();

        boolean found = invokeFillPointGet(streamResult, keyValues, false, Bytes.toBytes("f"),
            expectedRowKey, false);

        assertTrue(found);
        assertEquals(2, keyValues.size());
        assertArrayEquals(expectedRowKey, keyValues.get(0).getRow());
        assertArrayEquals(expectedRowKey, keyValues.get(1).getRow());
        assertTrue(keyValues.get(0) instanceof OHBaseResultCell);
        verify(streamResult, times(2)).getRow();
    }

    @Test
    public void testPointGetRejectsUnexpectedFirstRowKey() throws Exception {
        AbstractQueryStreamResult streamResult = stream(row("actual", "q1", 1L, "v1"));

        try {
            invokeFillPointGet(streamResult, new ArrayList<Cell>(), false, Bytes.toBytes("f"),
                Bytes.toBytes("expected"), false);
            fail("unexpected first rowkey must fail the point Get");
        } catch (InvocationTargetException e) {
            assertTrue(e.getCause() instanceof ObTableUnexpectedException);
        }
    }

    @Test
    public void testPointGetConsumesCompactBatchWithoutMaterializingRows() throws Exception {
        byte[] expectedRowKey = Bytes.toBytes("row-1");
        ObHBaseCellBatch firstBatch = compactBatch(row("row-1", "q1", 3L, "v1"));
        ObHBaseCellBatch secondBatch = compactBatch(row("unexpected-later-row", "q2", 2L,
            "v2"));
        AbstractQueryStreamResult streamResult = compactPointGetStream(compactRow(firstBatch),
            compactRow(secondBatch));
        List<Cell> keyValues = new ArrayList<Cell>();

        boolean found = invokeFillPointGet(streamResult, keyValues, false, Bytes.toBytes("f"),
            expectedRowKey, false);

        assertTrue(found);
        assertEquals(2, keyValues.size());
        assertArrayEquals(expectedRowKey, keyValues.get(0).getRow());
        assertArrayEquals(expectedRowKey, keyValues.get(1).getRow());
        assertArrayEquals(Bytes.toBytes("q2"), keyValues.get(1).getQualifier());
        verify(streamResult, times(3)).next();
        verify(streamResult, times(2)).drainCurrentHBaseRow();
        verify(streamResult, never()).getRow();
        verify(streamResult, never()).getCurrentHBaseCellBatch();
        verify(streamResult, never()).getCurrentHBaseCellIndex();
    }

    @Test
    public void testPointGetDrainsSameRowAcrossCachedBatchesOnce() throws Exception {
        byte[] expectedRowKey = Bytes.toBytes("row-1");
        ObHBaseCellBatch firstBatch = compactBatch(row("row-1", "q1", 4L, "v1"),
            row("row-1", "q2", 3L, "v2"));
        ObHBaseCellBatch secondBatch = compactBatch(row("row-1", "q3", 2L, "v3"),
            row("row-1", "q4", 1L, "v4"));
        AbstractQueryStreamResult streamResult = compactPointGetStream(compactRow(firstBatch,
            secondBatch));
        List<Cell> cells = new ArrayList<Cell>();

        boolean found = invokeFillPointGet(streamResult, cells, false, Bytes.toBytes("f"),
            expectedRowKey, false);

        assertTrue(found);
        assertEquals(4, cells.size());
        assertArrayEquals(Bytes.toBytes("q1"), cells.get(0).getQualifier());
        assertArrayEquals(Bytes.toBytes("q4"), cells.get(3).getQualifier());
        verify(streamResult, times(2)).next();
        verify(streamResult, times(1)).drainCurrentHBaseRow();
        verify(streamResult, never()).getRow();
    }

    @Test
    public void testDisabledLightweightCellUsesKeyValue() throws Exception {
        OHTable fallbackTable = new OHTable(Bytes.toBytes("test"), mock(ObTableClient.class),
            executorService, false);
        AbstractQueryStreamResult streamResult = stream(row("row-1", "q1", 1L, "v1"));
        List<Cell> cells = new ArrayList<Cell>();

        boolean found = invokeFillPointGet(fallbackTable, streamResult, cells, false,
            Bytes.toBytes("f"), Bytes.toBytes("row-1"), false);

        assertTrue(found);
        assertEquals(1, cells.size());
        assertTrue(cells.get(0) instanceof KeyValue);
    }

    @Test
    public void testPointGetExistenceOnlyDoesNotCreateKeyValue() throws Exception {
        AbstractQueryStreamResult streamResult = mock(AbstractQueryStreamResult.class);
        when(streamResult.next()).thenReturn(true);
        List<Cell> keyValues = new ArrayList<>();

        boolean found = invokeFillPointGet(streamResult, keyValues, false, Bytes.toBytes("f"),
            Bytes.toBytes("row-1"), true);

        assertTrue(found);
        assertTrue(keyValues.isEmpty());
        verify(streamResult, never()).drainCurrentHBaseRow();
        verify(streamResult, never()).getRow();
    }

    @Test
    public void testPointGetReturnsFalseForEmptyResult() throws Exception {
        AbstractQueryStreamResult streamResult = mock(AbstractQueryStreamResult.class);
        when(streamResult.next()).thenReturn(false);

        boolean found = invokeFillPointGet(streamResult, new ArrayList<Cell>(), false,
            Bytes.toBytes("f"), Bytes.toBytes("row-1"), false);

        assertFalse(found);
    }

    @Test
    public void testClosestRowBeforeKeepsOnlyCurrentMaxRow() throws Exception {
        AbstractQueryStreamResult streamResult = stream(row("row-1", "q1", 4L, "v1"),
            row("row-3", "q1", 3L, "v2"), row("row-3", "q2", 2L, "v3"),
            row("row-2", "q1", 1L, "v4"));
        List<Cell> keyValues = new ArrayList<>();

        boolean found = invokeGetMaxRow(streamResult, keyValues, false, Bytes.toBytes("f"), false);

        assertTrue(found);
        assertEquals(2, keyValues.size());
        assertArrayEquals(Bytes.toBytes("row-3"), keyValues.get(0).getRow());
        assertArrayEquals(Bytes.toBytes("row-3"), keyValues.get(1).getRow());
        assertArrayEquals(Bytes.toBytes("q1"), keyValues.get(0).getQualifier());
        assertArrayEquals(Bytes.toBytes("q2"), keyValues.get(1).getQualifier());
    }

    @Test
    public void testClosestRowBeforeExistenceOnlyDoesNotCreateKeyValue() throws Exception {
        AbstractQueryStreamResult streamResult = mock(AbstractQueryStreamResult.class);
        when(streamResult.next()).thenReturn(true);
        List<Cell> keyValues = new ArrayList<>();

        boolean found = invokeGetMaxRow(streamResult, keyValues, false, Bytes.toBytes("f"), true);

        assertTrue(found);
        assertTrue(keyValues.isEmpty());
        verify(streamResult, never()).getRow();
    }

    @Test
    public void testClosestRowBeforeConsumesCompactBatch() throws Exception {
        AbstractQueryStreamResult streamResult = compactStream(compactBatch(
            row("row-1", "q1", 4L, "v1"), row("row-3", "q1", 3L, "v2"),
            row("row-3", "q2", 2L, "v3"), row("row-2", "q1", 1L, "v4")));
        List<Cell> keyValues = new ArrayList<Cell>();

        boolean found = invokeGetMaxRow(streamResult, keyValues, false, Bytes.toBytes("f"), false);

        assertTrue(found);
        assertEquals(2, keyValues.size());
        assertArrayEquals(Bytes.toBytes("row-3"), keyValues.get(0).getRow());
        assertArrayEquals(Bytes.toBytes("q2"), keyValues.get(1).getQualifier());
        verify(streamResult, never()).drainCurrentHBaseRow();
        verify(streamResult, never()).getRow();
    }

    @Test
    public void testTableGroupSplitsFamilyAndQualifier() throws Exception {
        byte[] familyAndQualifier = Bytes.add(Bytes.toBytes("family"), new byte[] { 0 },
            Bytes.toBytes("qualifier"));
        AbstractQueryStreamResult streamResult = stream(row(Bytes.toBytes("row-1"),
            familyAndQualifier, 1L, Bytes.toBytes("value")));
        List<Cell> keyValues = new ArrayList<>();

        boolean found = invokeFillPointGet(streamResult, keyValues, true, new byte[0],
            Bytes.toBytes("row-1"), false);

        assertTrue(found);
        assertEquals(1, keyValues.size());
        assertArrayEquals(Bytes.toBytes("family"), keyValues.get(0).getFamily());
        assertArrayEquals(Bytes.toBytes("qualifier"), keyValues.get(0).getQualifier());
        assertSame(keyValues.get(0).getFamilyArray(), keyValues.get(0).getQualifierArray());
    }

    @Test
    public void testTableGroupSupportsEmptyQualifier() throws Exception {
        byte[] familyAndQualifier = Bytes.add(Bytes.toBytes("family"), new byte[] { 0 });
        AbstractQueryStreamResult streamResult = stream(row(Bytes.toBytes("row-1"),
            familyAndQualifier, 1L, Bytes.toBytes("value")));
        List<Cell> keyValues = new ArrayList<>();

        boolean found = invokeFillPointGet(streamResult, keyValues, true, new byte[0],
            Bytes.toBytes("row-1"), false);

        assertTrue(found);
        assertEquals(1, keyValues.size());
        assertArrayEquals(Bytes.toBytes("family"), keyValues.get(0).getFamily());
        assertArrayEquals(new byte[0], keyValues.get(0).getQualifier());
    }

    @Test
    public void testTableGroupSupportsMaximumFamilyLength() throws Exception {
        byte[] family = new byte[Byte.MAX_VALUE];
        Arrays.fill(family, (byte) 'f');
        byte[] qualifier = new byte[] { 0, (byte) 0xff, 1 };
        byte[] familyAndQualifier = Bytes.add(family, new byte[] { 0 }, qualifier);
        AbstractQueryStreamResult streamResult = stream(row(new byte[] { 0, (byte) 0xff },
            familyAndQualifier, 1L, Bytes.toBytes("value")));
        List<Cell> keyValues = new ArrayList<>();

        boolean found = invokeGetMaxRow(streamResult, keyValues, true, new byte[0], false);

        assertTrue(found);
        assertEquals(1, keyValues.size());
        assertArrayEquals(family, keyValues.get(0).getFamily());
        assertArrayEquals(qualifier, keyValues.get(0).getQualifier());
    }

    @Test
    public void testTableGroupRejectsMissingFamilyDelimiter() throws Exception {
        AbstractQueryStreamResult streamResult = stream(row(Bytes.toBytes("row-1"),
            Bytes.toBytes("family-without-delimiter"), 1L, Bytes.toBytes("value")));

        try {
            invokeFillPointGet(streamResult, new ArrayList<Cell>(), true, new byte[0],
                Bytes.toBytes("row-1"), false);
            fail("missing family delimiter must fail the TableGroup Get");
        } catch (InvocationTargetException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals("Cannot get family name", e.getCause().getMessage());
        }
    }

    @Test
    public void testBatchGetBuildsTableGroupKeyValuesFromCompositeQualifier() throws Exception {
        byte[] firstFamilyQualifier = Bytes.add(Bytes.toBytes("f1"), new byte[] { 0 },
            Bytes.toBytes("q1"));
        byte[] secondFamilyQualifier = Bytes.add(Bytes.toBytes("f2"), new byte[] { 0 },
            Bytes.toBytes("q2"));
        ObTableSingleOpEntity entity = mock(ObTableSingleOpEntity.class);
        when(entity.getPropertiesValues()).thenReturn(
            Arrays.asList(ObObj.getInstance(Bytes.toBytes("row-1")),
                ObObj.getInstance(firstFamilyQualifier), ObObj.getInstance(2L),
                ObObj.getInstance(Bytes.toBytes("v1")), ObObj.getInstance(Bytes.toBytes("row-1")),
                ObObj.getInstance(secondFamilyQualifier), ObObj.getInstance(1L),
                ObObj.getInstance(Bytes.toBytes("v2"))));
        ObTableSingleOpResult result = mock(ObTableSingleOpResult.class);
        when(result.getEntity()).thenReturn(entity);

        List<Cell> cells = invokeGenerateGetResult(result);

        assertEquals(2, cells.size());
        assertTrue(cells.get(0) instanceof OHBaseResultCell);
        assertArrayEquals(Bytes.toBytes("f1"), cells.get(0).getFamily());
        assertArrayEquals(Bytes.toBytes("q1"), cells.get(0).getQualifier());
        assertArrayEquals(Bytes.toBytes("f2"), cells.get(1).getFamily());
        assertArrayEquals(Bytes.toBytes("q2"), cells.get(1).getQualifier());
    }

    @Test
    public void testQueryAndMutateResultConsumesCompactBatch() throws Exception {
        ObTableQueryResult queryResult = compactQueryResult(row("row-1", "q1", 2L, "v1"),
            row("row-1", "q2", 1L, "v2"));
        List<KeyValue> keyValues = new ArrayList<KeyValue>();

        Method method = OHTable.class.getDeclaredMethod("addQueryResultToKeyValueList",
            ObTableQueryResult.class, List.class, byte[].class);
        method.setAccessible(true);
        method.invoke(table, queryResult, keyValues, Bytes.toBytes("f"));

        assertEquals(2, keyValues.size());
        assertArrayEquals(Bytes.toBytes("q1"), keyValues.get(0).getQualifier());
        assertArrayEquals(Bytes.toBytes("v2"), keyValues.get(1).getValue());
        assertTrue(queryResult.hasHBaseCellBatch());
    }

    @SuppressWarnings("unchecked")
    private List<Cell> invokeGenerateGetResult(ObTableSingleOpResult result) throws Exception {
        Method method = OHTable.class.getDeclaredMethod("generateGetResult",
            ObTableSingleOpResult.class);
        method.setAccessible(true);
        return (List<Cell>) method.invoke(table, result);
    }

    private boolean invokeFillPointGet(AbstractQueryStreamResult streamResult,
                                       List<Cell> keyValues, boolean isTableGroup, byte[] family,
                                       byte[] expectedRowKey, boolean checkExistenceOnly)
                                                                                         throws Exception {
        return invokeFillPointGet(table, streamResult, keyValues, isTableGroup, family,
            expectedRowKey, checkExistenceOnly);
    }

    private boolean invokeFillPointGet(OHTable targetTable, AbstractQueryStreamResult streamResult,
                                       List<Cell> keyValues, boolean isTableGroup, byte[] family,
                                       byte[] expectedRowKey, boolean checkExistenceOnly)
                                                                                         throws Exception {
        Method method = OHTable.class.getDeclaredMethod("fillPointGetFromResult",
            AbstractQueryStreamResult.class, List.class, boolean.class, byte[].class, byte[].class,
            boolean.class);
        method.setAccessible(true);
        return (Boolean) method.invoke(targetTable, streamResult, keyValues, isTableGroup, family,
            expectedRowKey, checkExistenceOnly);
    }

    private boolean invokeGetMaxRow(AbstractQueryStreamResult streamResult, List<Cell> keyValues,
                                    boolean isTableGroup, byte[] family, boolean checkExistenceOnly)
                                                                                                    throws Exception {
        Method method = OHTable.class
            .getDeclaredMethod("getMaxRowFromResult", AbstractQueryStreamResult.class, List.class,
                boolean.class, byte[].class, boolean.class);
        method.setAccessible(true);
        return (Boolean) method.invoke(table, streamResult, keyValues, isTableGroup, family,
            checkExistenceOnly);
    }

    private static AbstractQueryStreamResult stream(List<ObObj>... rows) throws Exception {
        AbstractQueryStreamResult streamResult = mock(AbstractQueryStreamResult.class);
        Boolean[] remaining = new Boolean[Math.max(0, rows.length - 1)];
        Arrays.fill(remaining, true);
        when(streamResult.next()).thenReturn(true, remaining).thenReturn(false);
        when(streamResult.getRow()).thenReturn(rows[0], Arrays.copyOfRange(rows, 1, rows.length));
        return streamResult;
    }

    private static AbstractQueryStreamResult compactStream(ObHBaseCellBatch batch)
                                                                                      throws Exception {
        AbstractQueryStreamResult streamResult = mock(AbstractQueryStreamResult.class);
        AtomicInteger index = new AtomicInteger(-1);
        when(streamResult.next()).thenAnswer(invocation -> index.incrementAndGet() < batch.size());
        when(streamResult.isCurrentHBaseCell()).thenReturn(true);
        when(streamResult.getCurrentHBaseCellBatch()).thenReturn(batch);
        when(streamResult.getCurrentHBaseCellIndex()).thenAnswer(invocation -> index.get());
        return streamResult;
    }

    private static AbstractQueryStreamResult compactPointGetStream(ObHBaseCellRow... rows)
                                                                                         throws Exception {
        AbstractQueryStreamResult streamResult = mock(AbstractQueryStreamResult.class);
        Boolean[] remaining = new Boolean[Math.max(0, rows.length - 1)];
        Arrays.fill(remaining, true);
        when(streamResult.next()).thenReturn(true, remaining).thenReturn(false);
        when(streamResult.isCurrentHBaseCell()).thenReturn(true);
        when(streamResult.drainCurrentHBaseRow()).thenReturn(rows[0], Arrays.copyOfRange(rows, 1,
            rows.length));
        return streamResult;
    }

    private static ObHBaseCellRow compactRow(ObHBaseCellBatch... batches) throws Exception {
        assertTrue(batches.length > 0);
        Constructor<ObHBaseCellRow> constructor = ObHBaseCellRow.class
            .getDeclaredConstructor(byte[].class);
        constructor.setAccessible(true);
        ObHBaseCellRow row = constructor.newInstance(batches[0].getRowKey(0));
        Method addSlice = ObHBaseCellRow.class.getDeclaredMethod("addSlice",
            ObHBaseCellBatch.class, int.class, int.class);
        addSlice.setAccessible(true);
        for (ObHBaseCellBatch batch : batches) {
            addSlice.invoke(row, batch, 0, batch.size());
        }
        return row;
    }

    @SafeVarargs
    private static ObHBaseCellBatch compactBatch(List<ObObj>... rows) {
        return compactQueryResult(rows).getHBaseCellBatch();
    }

    @SafeVarargs
    private static ObTableQueryResult compactQueryResult(List<ObObj>... rows) {
        ObTableQueryResult encodedResult = new ObTableQueryResult();
        encodedResult.addPropertiesName("K");
        encodedResult.addPropertiesName("Q");
        encodedResult.addPropertiesName("T");
        encodedResult.addPropertiesName("V");
        encodedResult.addAllPropertiesRows(Arrays.asList(rows));
        encodedResult.setRowCount(rows.length);

        ByteBuf buf = Unpooled.wrappedBuffer(encodedResult.encode());
        try {
            ObTableQueryResult decodedResult = new ObTableQueryResult();
            decodedResult.decode(buf);
            assertTrue(decodedResult.hasHBaseCellBatch());
            return decodedResult;
        } finally {
            buf.release();
        }
    }

    private static List<ObObj> row(String rowKey, String qualifier, long timestamp, String value) {
        return row(Bytes.toBytes(rowKey), Bytes.toBytes(qualifier), timestamp, Bytes.toBytes(value));
    }

    private static List<ObObj> row(byte[] rowKey, byte[] qualifier, long timestamp, byte[] value) {
        return Arrays.asList(binaryObj(rowKey), binaryObj(qualifier), new ObObj(new ObObjMeta(
            ObObjType.ObInt64Type, ObCollationLevel.CS_LEVEL_NUMERIC,
            ObCollationType.CS_TYPE_BINARY, (byte) 0), timestamp), binaryObj(value));
    }

    private static ObObj binaryObj(byte[] value) {
        return new ObObj(new ObObjMeta(ObObjType.ObVarcharType, ObCollationLevel.CS_LEVEL_EXPLICIT,
            ObCollationType.CS_TYPE_BINARY, (byte) 0), value);
    }
}
