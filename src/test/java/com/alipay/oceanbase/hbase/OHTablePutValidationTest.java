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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OHTablePutValidationTest {

    private static final byte[] ROW    = Bytes.toBytes("row");
    private static final byte[] FAMILY = Bytes.toBytes("cf");

    private ExecutorService     executorService;

    @Before
    public void setUp() {
        executorService = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() {
        executorService.shutdownNow();
    }

    @Test
    public void testStaticValidationDoesNotMaterializeDeprecatedFamilyMap() {
        Put put = newPut(FAMILY, "q", "value");
        Cell cell = put.getFamilyCellMap().get(FAMILY).get(0);

        OHTable.validatePut(put, KeyValueUtil.length(cell));
    }

    @Test
    public void testTableValidationDoesNotMaterializeDeprecatedFamilyMap() {
        OHTable table = new OHTable(Bytes.toBytes("test"), mock(ObTableClient.class),
            executorService);

        table.validatePutMutation(newPut(FAMILY, "q", "value"));
    }

    @Test
    public void testCellAtMaximumSizeIsAccepted() {
        Put put = newPut(FAMILY, "q", "value");
        Cell cell = put.getFamilyCellMap().get(FAMILY).get(0);

        OHTable.validatePut(put, KeyValueUtil.length(cell));
    }

    @Test
    public void testCellOverMaximumSizeIsRejected() {
        Put put = newPut(FAMILY, "q", "value");
        Cell cell = put.getFamilyCellMap().get(FAMILY).get(0);

        try {
            OHTable.validatePut(put, KeyValueUtil.length(cell) - 1);
            fail("oversized cell should fail");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("KeyValue size too large"));
        }
    }

    @Test
    public void testEmptyPutIsRejected() {
        try {
            OHTable.validatePut(new NoDeprecatedFamilyMapPut(ROW), -1);
            fail("empty put should fail");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("No columns to insert"));
        }
    }

    @Test
    public void testMultipleFamiliesUseOriginalCellMap() {
        Put put = newPut(FAMILY, "q1", "value1");
        put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("q2"), Bytes.toBytes("value2"));

        OHTable.validatePut(put, Integer.MAX_VALUE);
    }

    @Test
    public void testNonKeyValueCellUsesCalculatedLengthWithoutConversion() {
        Put put = newPut(FAMILY, "q", "value");
        Cell cell = mock(Cell.class);
        when(cell.getRowLength()).thenReturn((short) 3);
        when(cell.getFamilyLength()).thenReturn((byte) 2);
        when(cell.getQualifierLength()).thenReturn(1);
        when(cell.getValueLength()).thenReturn(5);
        when(cell.getTagsLength()).thenReturn(0);
        put.getFamilyCellMap().get(FAMILY).set(0, cell);

        OHTable.validatePut(put, KeyValueUtil.length(cell));
    }

    private static Put newPut(byte[] family, String qualifier, String value) {
        Put put = new NoDeprecatedFamilyMapPut(ROW);
        put.addColumn(family, Bytes.toBytes(qualifier), Bytes.toBytes(value));
        return put;
    }

    private static final class NoDeprecatedFamilyMapPut extends Put {

        NoDeprecatedFamilyMapPut(byte[] row) {
            super(row);
        }

        @Override
        @Deprecated
        public NavigableMap<byte[], List<KeyValue>> getFamilyMap() {
            throw new AssertionError("Put validation must not materialize getFamilyMap()");
        }
    }
}
