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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Objects;

/**
 * Immutable read-result cell backed by the decoded HBase field arrays.
 */
@InterfaceAudience.Private
public final class OHBaseResultCell implements Cell {

    private static final byte   PUT_TYPE = KeyValue.Type.Put.getCode();
    private static final byte[] EMPTY    = HConstants.EMPTY_BYTE_ARRAY;

    private final byte[]        row;
    private final byte[]        familyArray;
    private final int           familyOffset;
    private final int           familyLength;
    private final byte[]        qualifierArray;
    private final int           qualifierOffset;
    private final int           qualifierLength;
    private final long          timestamp;
    private final byte[]        value;

    public static OHBaseResultCell create(byte[] row, byte[] family, byte[] qualifier,
                                          long timestamp, byte[] value) {
        return new OHBaseResultCell(row, family, 0, length(family), qualifier, 0,
            length(qualifier), timestamp, value);
    }

    public static OHBaseResultCell createTableGroup(byte[] row, byte[] familyQualifier,
                                                    long timestamp, byte[] value) {
        Objects.requireNonNull(familyQualifier, "familyQualifier is null");
        int familyLength = findFamilyDelimiter(familyQualifier);
        int qualifierOffset = familyLength + 1;
        return new OHBaseResultCell(row, familyQualifier, 0, familyLength, familyQualifier,
            qualifierOffset, familyQualifier.length - qualifierOffset, timestamp, value);
    }

    private OHBaseResultCell(byte[] row, byte[] familyArray, int familyOffset, int familyLength,
                             byte[] qualifierArray, int qualifierOffset, int qualifierLength,
                             long timestamp, byte[] value) {
        this.row = Objects.requireNonNull(row, "row is null");
        this.familyArray = Objects.requireNonNull(familyArray, "family is null");
        this.qualifierArray = Objects.requireNonNull(qualifierArray, "qualifier is null");
        this.value = value == null ? EMPTY : value;
        checkRange(familyArray, familyOffset, familyLength, "family");
        checkRange(qualifierArray, qualifierOffset, qualifierLength, "qualifier");
        if (row.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("row length " + row.length + " exceeds "
                                               + Short.MAX_VALUE);
        }
        if (familyLength > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("family length " + familyLength + " exceeds "
                                               + Byte.MAX_VALUE);
        }
        this.familyOffset = familyOffset;
        this.familyLength = familyLength;
        this.qualifierOffset = qualifierOffset;
        this.qualifierLength = qualifierLength;
        this.timestamp = timestamp;
    }

    private static int length(byte[] value) {
        return value == null ? 0 : value.length;
    }

    private static void checkRange(byte[] array, int offset, int length, String field) {
        if (offset < 0 || length < 0 || offset > array.length - length) {
            throw new IndexOutOfBoundsException(field + " range is out of bounds");
        }
    }

    private static int findFamilyDelimiter(byte[] familyQualifier) {
        for (int i = 0; i < familyQualifier.length; i++) {
            if (familyQualifier[i] == '\0') {
                return i;
            }
        }
        throw new RuntimeException("Cannot get family name");
    }

    @Override
    public byte[] getRowArray() {
        return row;
    }

    @Override
    public int getRowOffset() {
        return 0;
    }

    @Override
    public short getRowLength() {
        return (short) row.length;
    }

    @Override
    public byte[] getFamilyArray() {
        return familyArray;
    }

    @Override
    public int getFamilyOffset() {
        return familyOffset;
    }

    @Override
    public byte getFamilyLength() {
        return (byte) familyLength;
    }

    @Override
    public byte[] getQualifierArray() {
        return qualifierArray;
    }

    @Override
    public int getQualifierOffset() {
        return qualifierOffset;
    }

    @Override
    public int getQualifierLength() {
        return qualifierLength;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public byte getTypeByte() {
        return PUT_TYPE;
    }

    public long getMvccVersion() {
        return 0L;
    }

    @Override
    public long getSequenceId() {
        return 0L;
    }

    @Override
    public byte[] getValueArray() {
        return value;
    }

    @Override
    public int getValueOffset() {
        return 0;
    }

    @Override
    public int getValueLength() {
        return value.length;
    }

    @Override
    public byte[] getTagsArray() {
        return EMPTY;
    }

    @Override
    public int getTagsOffset() {
        return 0;
    }

    @Override
    public int getTagsLength() {
        return 0;
    }

    public byte[] getValue() {
        return Bytes.copy(value, 0, value.length);
    }

    public byte[] getFamily() {
        return Bytes.copy(familyArray, familyOffset, familyLength);
    }

    public byte[] getQualifier() {
        return Bytes.copy(qualifierArray, qualifierOffset, qualifierLength);
    }

    public byte[] getRow() {
        return Bytes.copy(row, 0, row.length);
    }
}
