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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_PUT_COMPACT_CELL_DEFAULT;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_PUT_COMPACT_CELL_ENABLED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    public void testConfigNameAndDefault() throws Exception {
        assertEquals("hbase.htable.put.compact.cell.enabled", HBASE_HTABLE_PUT_COMPACT_CELL_ENABLED);
        assertTrue(HBASE_HTABLE_PUT_COMPACT_CELL_DEFAULT);
        OHTable table = newTable();
        assertTrue(getCompactEnabled(table));
    }

    @Test
    public void testCompactRequestMatchesLegacyWithoutTtl() throws Exception {
        Put put = new Put(Bytes.toBytes("row"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q1"), 1001L, Bytes.toBytes("value1"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qualifier-2"), 1002L,
            Bytes.toBytes("value-2"));
        assertLegacyAndCompactEqual(put, OHOperationType.PUT);
    }

    @Test
    public void testCompactRequestMatchesLegacyWithTtlAndMultipleRows() throws Exception {
        Put first = new Put(Bytes.toBytes("row-1"));
        first.setTTL(60000L);
        first.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q1"), 2001L, Bytes.toBytes("value-1"));
        first.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q2"), 2002L, Bytes.toBytes("value-2"));
        first.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("q3"), 2003L, Bytes.toBytes("value-3"));
        Put second = new Put(Bytes.toBytes("row-2"));
        second.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q4"), 3001L, Bytes.toBytes("value-4"));

        OHTable legacyTable = newTable();
        OHTable compactTable = newTable();
        setCompactEnabled(legacyTable, false);
        setCompactEnabled(compactTable, true);
        ObHbaseRequest legacy = legacyTable.buildHbaseRequest(Arrays.asList(first, second),
            OHOperationType.PUT_LIST);
        ObHbaseRequest compact = compactTable.buildHbaseRequest(Arrays.asList(first, second),
            OHOperationType.PUT_LIST);

        assertEquals(2, legacy.getCfRows().size());
        assertEquals(2, compact.getCfRows().size());
        for (int i = 0; i < compact.getCfRows().size(); i++) {
            assertFalse(legacy.getCfRows().get(i).hasCompactCells());
            assertTrue(compact.getCfRows().get(i).hasCompactCells());
        }
        assertArrayEquals(legacy.encode(), compact.encode());
    }

    @Test
    public void testBufferedCompactRequestOwnsQualifierAndValueBytes() throws Exception {
        Put put = new Put(Bytes.toBytes("row"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qualifier"), 4001L,
            Bytes.toBytes("original-value"));
        Cell source = put.getFamilyCellMap().get(Bytes.toBytes("cf")).get(0);

        OHTable compactTable = newTable();
        compactTable.setAutoFlush(false);
        setCompactEnabled(compactTable, true);
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

    private void assertLegacyAndCompactEqual(Put put, OHOperationType operationType)
                                                                                    throws Exception {
        OHTable legacyTable = newTable();
        OHTable compactTable = newTable();
        setCompactEnabled(legacyTable, false);
        setCompactEnabled(compactTable, true);
        ObHbaseRequest legacy = legacyTable.buildHbaseRequest(Collections.singletonList(put),
            operationType);
        ObHbaseRequest compact = compactTable.buildHbaseRequest(Collections.singletonList(put),
            operationType);
        assertFalse(legacy.getCfRows().get(0).hasCompactCells());
        assertTrue(compact.getCfRows().get(0).hasCompactCells());
        assertArrayEquals(legacy.encode(), compact.encode());
    }

    private OHTable newTable() {
        return new OHTable(Bytes.toBytes("test"), mock(ObTableClient.class), executorService, true);
    }

    private static boolean getCompactEnabled(OHTable table) throws Exception {
        Field field = OHTable.class.getDeclaredField("enablePutCompactCell");
        field.setAccessible(true);
        return field.getBoolean(table);
    }

    private static void setCompactEnabled(OHTable table, boolean enabled) throws Exception {
        Field field = OHTable.class.getDeclaredField("enablePutCompactCell");
        field.setAccessible(true);
        field.setBoolean(table, enabled);
    }
}
