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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.*;

public class OHBaseResultCellTest {

    @Test
    public void testFieldBackedCellAccessors() {
        byte[] row = bytes("row-1");
        byte[] family = bytes("f");
        byte[] qualifier = bytes("q1");
        byte[] value = bytes("value-1");
        OHBaseResultCell cell = OHBaseResultCell.create(row, family, qualifier, 123L, value);

        assertSame(row, cell.getRowArray());
        assertSame(family, cell.getFamilyArray());
        assertSame(qualifier, cell.getQualifierArray());
        assertSame(value, cell.getValueArray());
        assertEquals(0, cell.getRowOffset());
        assertEquals(row.length, cell.getRowLength());
        assertEquals(0, cell.getFamilyOffset());
        assertEquals(family.length, cell.getFamilyLength());
        assertEquals(0, cell.getQualifierOffset());
        assertEquals(qualifier.length, cell.getQualifierLength());
        assertEquals(0, cell.getValueOffset());
        assertEquals(value.length, cell.getValueLength());
        assertEquals(123L, cell.getTimestamp());
        assertEquals(KeyValue.Type.Put.getCode(), cell.getTypeByte());
        assertEquals(0L, cell.getMvccVersion());
        assertEquals(0L, cell.getSequenceId());
        assertEquals(0, cell.getTagsLength());

        assertArrayEquals(row, cell.getRow());
        assertArrayEquals(family, cell.getFamily());
        assertArrayEquals(qualifier, cell.getQualifier());
        assertArrayEquals(value, cell.getValue());
        assertNotSame(row, cell.getRow());
        assertNotSame(value, cell.getValue());
    }

    @Test
    public void testTableGroupCellUsesSharedArrayRanges() {
        byte[] familyQualifier = new byte[] { 'f', '1', 0, 'q', '1' };
        OHBaseResultCell cell = OHBaseResultCell.createTableGroup(bytes("row-1"), familyQualifier,
            99L, bytes("v"));

        assertSame(familyQualifier, cell.getFamilyArray());
        assertSame(familyQualifier, cell.getQualifierArray());
        assertEquals(0, cell.getFamilyOffset());
        assertEquals(2, cell.getFamilyLength());
        assertEquals(3, cell.getQualifierOffset());
        assertEquals(2, cell.getQualifierLength());
        assertArrayEquals(bytes("f1"), CellUtil.cloneFamily(cell));
        assertArrayEquals(bytes("q1"), CellUtil.cloneQualifier(cell));
    }

    @Test
    public void testResultAndLegacyKeyValueApisRemainCompatible() {
        byte[] family = bytes("f");
        byte[] qualifier = bytes("q");
        byte[] value = bytes("value");
        Cell cell = OHBaseResultCell.create(bytes("row"), family, qualifier, 7L, value);
        Result result = Result.create(Collections.singletonList(cell));

        assertSame(cell, result.rawCells()[0]);
        assertSame(cell, result.listCells().get(0));
        assertArrayEquals(value, result.getValue(family, qualifier));
        ByteBuffer valueBuffer = result.getValueAsByteBuffer(family, qualifier);
        assertArrayEquals(value, Bytes.toBytes(valueBuffer));
        assertSame(cell, result.getColumnLatestCell(family, qualifier));
        assertEquals(1, result.getColumnCells(family, qualifier).size());
        assertArrayEquals(value, result.getFamilyMap(family).get(qualifier));

        KeyValue converted = KeyValueUtil.ensureKeyValue(cell);
        assertArrayEquals(bytes("row"), CellUtil.cloneRow(converted));
        assertArrayEquals(family, CellUtil.cloneFamily(converted));
        assertArrayEquals(qualifier, CellUtil.cloneQualifier(converted));
        assertArrayEquals(value, CellUtil.cloneValue(converted));
        assertEquals(7L, converted.getTimestamp());
    }

    @Test(expected = RuntimeException.class)
    public void testTableGroupCellRejectsMissingDelimiter() {
        OHBaseResultCell.createTableGroup(bytes("row"), bytes("family-qualifier"), 1L, bytes("v"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCellRejectsOversizedFamily() {
        OHBaseResultCell.create(bytes("row"), new byte[Byte.MAX_VALUE + 1], bytes("q"), 1L,
            bytes("v"));
    }

    private static byte[] bytes(String value) {
        return Bytes.toBytes(value);
    }
}
