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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_PUT_DIRECT_AUTOFLUSH_DEFAULT;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_PUT_DIRECT_AUTOFLUSH_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class OHTablePutDirectAutoFlushTest {

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
    public void testConfigNameAndDefault() {
        assertEquals("hbase.htable.put.direct.autoflush.enabled",
            HBASE_HTABLE_PUT_DIRECT_AUTOFLUSH_ENABLED);
        assertTrue(HBASE_HTABLE_PUT_DIRECT_AUTOFLUSH_DEFAULT);
    }

    @Test
    public void testDefaultEnablesDirectAutoFlush() throws Exception {
        OHTable table = new OHTable(Bytes.toBytes("test"), mock(ObTableClient.class),
            executorService, true);
        assertTrue(table.isPutDirectAutoFlushEnabled());
        assertTrue(table.isWriteBufferEmpty());
    }

    @Test
    public void testConfigCanDisableDirectAutoFlush() throws Exception {
        OHTable table = new OHTable(Bytes.toBytes("test"), mock(ObTableClient.class),
            executorService, true);
        Field enabled = OHTable.class.getDeclaredField("enablePutDirectAutoFlush");
        enabled.setAccessible(true);
        enabled.setBoolean(table, false);
        assertFalse(table.isPutDirectAutoFlushEnabled());
    }

    @Test
    public void testAutoFlushSinglePutBypassesMutator() throws Exception {
        CapturingOHTable table = new CapturingOHTable(Bytes.toBytes("t"),
            mock(ObTableClient.class), executorService);
        Put put = newPut("row1", "cf", "q", "v");

        table.put(put);

        assertEquals(1, table.directBatchCalls.get());
        assertEquals(0, table.legacyMutateFlushes.get());
        assertEquals(1, table.lastActions.size());
        assertEquals(OHOperationType.PUT, table.lastOpType);
        assertTrue(table.isWriteBufferEmpty());
        assertNull(getMutator(table));
    }

    @Test
    public void testAutoFlushPutListBypassesMutator() throws Exception {
        CapturingOHTable table = new CapturingOHTable(Bytes.toBytes("t"),
            mock(ObTableClient.class), executorService);
        List<Put> puts = Arrays
            .asList(newPut("r1", "cf", "q", "v1"), newPut("r2", "cf", "q", "v2"));

        table.put(puts);

        assertEquals(1, table.directBatchCalls.get());
        assertEquals(0, table.legacyMutateFlushes.get());
        assertEquals(2, table.lastActions.size());
        assertEquals(OHOperationType.PUT_LIST, table.lastOpType);
        assertNull(getMutator(table));
    }

    @Test
    public void testAutoFlushFalseUsesBufferedMutator() throws Exception {
        CapturingOHTable table = new CapturingOHTable(Bytes.toBytes("t"),
            mock(ObTableClient.class), executorService);
        table.setAutoFlush(false);
        Put put = newPut("row1", "cf", "q", "v");

        table.put(put);

        assertEquals(0, table.directBatchCalls.get());
        assertFalse(table.isWriteBufferEmpty());
        assertTrue(getMutator(table) != null);
        assertEquals(1, getMutator(table).getCurrentBufferSize() > 0 ? 1 : 0);

        table.flushCommits();
        assertEquals(1, table.directBatchCalls.get()); // flush -> innerBatchImpl
        assertTrue(table.isWriteBufferEmpty());
    }

    @Test
    public void testPendingBufferFlushedBeforeDirectPut() throws Exception {
        CapturingOHTable table = new CapturingOHTable(Bytes.toBytes("t"),
            mock(ObTableClient.class), executorService);
        table.setAutoFlush(false);
        Put buffered = newPut("old", "cf", "q", "v0");
        table.put(buffered);
        assertFalse(table.isWriteBufferEmpty());

        table.setAutoFlush(true);
        Put direct = newPut("new", "cf", "q", "v1");
        table.put(direct);

        // First call from flush of pending, second from direct put.
        assertEquals(2, table.directBatchCalls.get());
        assertEquals(OHOperationType.PUT, table.lastOpType);
        assertEquals(1, table.lastActions.size());
        assertTrue(Bytes.equals(((Put) table.lastActions.get(0)).getRow(), Bytes.toBytes("new")));
        assertTrue(table.isWriteBufferEmpty());
    }

    @Test
    public void testEmptyPutRejectedOnDirectPath() throws Exception {
        CapturingOHTable table = new CapturingOHTable(Bytes.toBytes("t"),
            mock(ObTableClient.class), executorService);
        try {
            table.put(new Put(Bytes.toBytes("row")));
            fail("empty put should fail");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("No columns"));
        }
        assertEquals(0, table.directBatchCalls.get());
        assertNull(getMutator(table));
    }

    @Test
    public void testInvalidDirectPutDoesNotFlushPendingBuffer() throws Exception {
        CapturingOHTable table = new CapturingOHTable(Bytes.toBytes("t"),
            mock(ObTableClient.class), executorService);
        table.setAutoFlush(false);
        table.put(newPut("old", "cf", "q", "v"));
        assertFalse(table.isWriteBufferEmpty());

        table.setAutoFlush(true);
        try {
            table.put(new Put(Bytes.toBytes("invalid")));
            fail("empty put should fail");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("No columns"));
        }

        assertEquals(0, table.directBatchCalls.get());
        assertFalse(table.isWriteBufferEmpty());
    }

    @Test
    public void testDirectDisabledFallsBackToMutator() throws Exception {
        CapturingOHTable table = new CapturingOHTable(Bytes.toBytes("t"),
            mock(ObTableClient.class), executorService);
        Field enabled = OHTable.class.getDeclaredField("enablePutDirectAutoFlush");
        enabled.setAccessible(true);
        enabled.setBoolean(table, false);

        table.put(newPut("row1", "cf", "q", "v"));

        assertEquals(1, table.directBatchCalls.get()); // via flush path after mutate
        assertTrue(getMutator(table) != null);
    }

    private static Put newPut(String row, String family, String qualifier, String value) {
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        return put;
    }

    private static com.alipay.oceanbase.hbase.util.OHBufferedMutatorImpl getMutator(OHTable table)
                                                                                                  throws Exception {
        Field field = OHTable.class.getDeclaredField("mutator");
        field.setAccessible(true);
        return (com.alipay.oceanbase.hbase.util.OHBufferedMutatorImpl) field.get(table);
    }

    /**
     * Captures innerBatchImpl invocations without talking to OceanBase.
     * Legacy mutate+flush still ends in innerBatchImpl, so call counts distinguish
     * "mutator never created" vs "buffer then flush".
     */
    private static final class CapturingOHTable extends OHTable {
        final AtomicInteger          directBatchCalls    = new AtomicInteger();
        final AtomicInteger          legacyMutateFlushes = new AtomicInteger();
        volatile List<? extends Row> lastActions         = Collections.emptyList();
        volatile OHOperationType     lastOpType;

        CapturingOHTable(byte[] tableName, ObTableClient client, ExecutorService pool) {
            super(tableName, client, pool, true);
        }

        @Override
        public void innerBatchImpl(final List<? extends Row> actions, final Object[] results,
                                   final OHOperationType opType) throws IOException {
            directBatchCalls.incrementAndGet();
            lastActions = new ArrayList<Row>(actions);
            lastOpType = opType;
            if (results != null) {
                for (int i = 0; i < results.length; i++) {
                    results[i] = org.apache.hadoop.hbase.client.Result.EMPTY_RESULT;
                }
            }
        }
    }
}
