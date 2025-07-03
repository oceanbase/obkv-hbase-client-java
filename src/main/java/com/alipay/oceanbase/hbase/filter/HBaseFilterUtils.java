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

package com.alipay.oceanbase.hbase.filter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.lang.reflect.Field;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;

@InterfaceAudience.Private
public class HBaseFilterUtils {

    public static byte[] toParseableByteArray(Filter filter) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        toParseableByteArray(byteStream, filter);
        return byteStream.toByteArray();
    }

    private static void toParseableByteArray(ByteArrayOutputStream byteStream, Filter filter)
                                                                                             throws IOException {
        if (filter == null) {
            throw new IllegalArgumentException("Filter is null");
        } else if (filter instanceof DependentColumnFilter) {
            toParseableByteArray(byteStream, (DependentColumnFilter) filter);
        } else if (filter instanceof CompareFilter) {
            // RowFilter, ValueFilter, QualifierFilter, FamilyFilter
            toParseableByteArray(byteStream, (CompareFilter) filter);
        } else if (filter instanceof SingleColumnValueExcludeFilter) {
            toParseableByteArray(byteStream, (SingleColumnValueExcludeFilter) filter);
        } else if (filter instanceof SingleColumnValueFilter) {
            toParseableByteArray(byteStream, (SingleColumnValueFilter) filter);
        } else if (filter instanceof PageFilter) {
            toParseableByteArray(byteStream, (PageFilter) filter);
        } else if (filter instanceof ColumnCountGetFilter) {
            toParseableByteArray(byteStream, (ColumnCountGetFilter) filter);
        } else if (filter instanceof FirstKeyValueMatchingQualifiersFilter) {
            toParseableByteArray(byteStream, (FirstKeyValueMatchingQualifiersFilter) filter);
        } else if (filter instanceof PrefixFilter) {
            toParseableByteArray(byteStream, (PrefixFilter) filter);
        } else if (filter instanceof FilterList) {
            toParseableByteArray(byteStream, (FilterList) filter);
        } else if (filter instanceof RandomRowFilter) {
            toParseableByteArray(byteStream, (RandomRowFilter) filter);
        } else if (filter instanceof ColumnPaginationFilter) {
            toParseableByteArray(byteStream, (ColumnPaginationFilter) filter);
        } else if (filter instanceof ColumnPrefixFilter) {
            toParseableByteArray(byteStream, (ColumnPrefixFilter) filter);
        } else if (filter instanceof FirstKeyOnlyFilter) {
            toParseableByteArray(byteStream, (FirstKeyOnlyFilter) filter);
        } else if (filter instanceof KeyOnlyFilter) {
            toParseableByteArray(byteStream, (KeyOnlyFilter) filter);
        } else if (filter instanceof FuzzyRowFilter) {
            toParseableByteArray(byteStream, (FuzzyRowFilter) filter);
        } else if (filter instanceof TimestampsFilter) {
            toParseableByteArray(byteStream, (TimestampsFilter) filter);
        } else if (filter instanceof MultiRowRangeFilter) {
            toParseableByteArray(byteStream, (MultiRowRangeFilter) filter);
        } else if (filter instanceof InclusiveStopFilter) {
            toParseableByteArray(byteStream, (InclusiveStopFilter) filter);
        } else if (filter instanceof ColumnRangeFilter) {
            toParseableByteArray(byteStream, (ColumnRangeFilter) filter);
        } else if (filter instanceof MultipleColumnPrefixFilter) {
            toParseableByteArray(byteStream, (MultipleColumnPrefixFilter) filter);
        } else if (filter instanceof SkipFilter) {
            toParseableByteArray(byteStream, (SkipFilter) filter);
        } else if (filter instanceof WhileMatchFilter) {
            toParseableByteArray(byteStream, (WhileMatchFilter) filter);
        } else {
            throw new IllegalArgumentException("Invalid filter: " + filter);
        }
    }

    public static byte[] toParseableByteArray(CompareFilter.CompareOp op) {
        if (op == null) {
            throw new IllegalArgumentException("Compare operator is null");
        }
        switch (op) {
            case LESS:
                return ParseConstants.LESS_THAN_ARRAY;
            case LESS_OR_EQUAL:
                return ParseConstants.LESS_THAN_OR_EQUAL_TO_ARRAY;
            case EQUAL:
                return ParseConstants.EQUAL_TO_ARRAY;
            case NOT_EQUAL:
                return ParseConstants.NOT_EQUAL_TO_ARRAY;
            case GREATER_OR_EQUAL:
                return ParseConstants.GREATER_THAN_OR_EQUAL_TO_ARRAY;
            case GREATER:
                return ParseConstants.GREATER_THAN_ARRAY;
            default:
                throw new IllegalArgumentException("Invalid compare operator: " + op);
        }
    }

    public static byte[] toParseableByteArray(CompareOperator op) {
        if (op == null) {
            throw new IllegalArgumentException("Compare operator is null");
        }
        switch (op) {
            case LESS:
                return ParseConstants.LESS_THAN_ARRAY;
            case LESS_OR_EQUAL:
                return ParseConstants.LESS_THAN_OR_EQUAL_TO_ARRAY;
            case EQUAL:
                return ParseConstants.EQUAL_TO_ARRAY;
            case NOT_EQUAL:
                return ParseConstants.NOT_EQUAL_TO_ARRAY;
            case GREATER_OR_EQUAL:
                return ParseConstants.GREATER_THAN_OR_EQUAL_TO_ARRAY;
            case GREATER:
                return ParseConstants.GREATER_THAN_ARRAY;
            default:
                throw new IllegalArgumentException("Invalid compare operator: " + op);
        }
    }

    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             ByteArrayComparable comparator) throws IOException {
        if (comparator == null) {
            throw new IllegalArgumentException("Comparator is null");
        }
        byteStream.write('\'');
        if (comparator instanceof BinaryComparator) {
            byteStream.write(ParseConstants.binaryType);
        } else if (comparator instanceof BinaryPrefixComparator) {
            byteStream.write(ParseConstants.binaryPrefixType);
        } else if (comparator instanceof RegexStringComparator) {
            byteStream.write(ParseConstants.regexStringType);
        } else if (comparator instanceof SubstringComparator) {
            byteStream.write(ParseConstants.substringType);
        } else {
            throw new IllegalArgumentException("This comparator has not been implemented "
                                               + comparator);
        }
        byteStream.write(':');
        writeBytesWithEscape(byteStream, comparator.getValue());
        byteStream.write('\'');
    }

    // CompareFilter(=,'binary:123')
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, CompareFilter filter)
                                                                                                    throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        byteStream.write(toParseableByteArray(filter.getOperator()));
        byteStream.write(',');
        toParseableByteArray(byteStream, filter.getComparator());
        byteStream.write(')');
    }

    // SingleColumnValueFilter('cf1','col1',=,'binary:123',true,true)
    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             SingleColumnValueFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write("('".getBytes());
        writeBytesWithEscape(byteStream, filter.getFamily());
        byteStream.write("','".getBytes());
        writeBytesWithEscape(byteStream, filter.getQualifier());
        byteStream.write("',".getBytes());
        byteStream.write(toParseableByteArray(filter.getOperator()));
        byteStream.write(',');
        toParseableByteArray(byteStream, filter.getComparator());
        byteStream.write(',');
        byteStream.write(Boolean.toString(filter.getFilterIfMissing()).getBytes());
        byteStream.write(',');
        byteStream.write(Boolean.toString(filter.getLatestVersionOnly()).getBytes());
        byteStream.write(')');
    }

    // SingleColumnValueExcludeFilter('cf1','col1',=,'binary:123',true,true)
    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             SingleColumnValueExcludeFilter filter)
                                                                                   throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write("('".getBytes());
        writeBytesWithEscape(byteStream, filter.getFamily());
        byteStream.write("','".getBytes());
        writeBytesWithEscape(byteStream, filter.getQualifier());
        byteStream.write("',".getBytes());
        byteStream.write(toParseableByteArray(filter.getOperator()));
        byteStream.write(',');
        toParseableByteArray(byteStream, filter.getComparator());
        byteStream.write(',');
        byteStream.write(Boolean.toString(filter.getFilterIfMissing()).getBytes());
        byteStream.write(',');
        byteStream.write(Boolean.toString(filter.getLatestVersionOnly()).getBytes());
        byteStream.write(')');
    }

    // PageFilter(100);
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, PageFilter filter)
                                                                                                 throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        byteStream.write(Long.toString(filter.getPageSize()).getBytes());
        byteStream.write(')');
    }

    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             RandomRowFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        byteStream.write(Integer.toString(Bytes.toInt(Bytes.toBytes(filter.getChance())))
            .getBytes());
        byteStream.write(')');
    }

    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             DependentColumnFilter filter) throws IOException {
        // DependentColumnFilter '(' family ',' qualifier ',' BOOL_VALUE ')'
        if (filter.getComparator() == null) {
            byteStream.write(filter.getClass().getSimpleName().getBytes());
            byteStream.write("('".getBytes());
            writeBytesWithEscape(byteStream, filter.getFamily());
            byteStream.write("','".getBytes());
            writeBytesWithEscape(byteStream, filter.getQualifier());
            byteStream.write("',".getBytes());
            byteStream.write(Boolean.toString(filter.getDropDependentColumn()).getBytes());
            byteStream.write(')');
        } else { // DependentColumnFilter '(' family ',' qualifier ',' BOOL_VALUE ',' compare_op ',' comparator ')'
            byteStream.write(filter.getClass().getSimpleName().getBytes());
            byteStream.write("('".getBytes());
            writeBytesWithEscape(byteStream, filter.getFamily());
            byteStream.write("','".getBytes());
            writeBytesWithEscape(byteStream, filter.getQualifier());
            byteStream.write("',".getBytes());
            byteStream.write(Boolean.toString(filter.getDropDependentColumn()).getBytes());
            byteStream.write(',');
            byteStream.write(toParseableByteArray(filter.getOperator()));
            byteStream.write(',');
            toParseableByteArray(byteStream, filter.getComparator());
            byteStream.write(')');
        }
    }

    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             ColumnPaginationFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        byteStream.write(Long.toString(filter.getLimit()).getBytes());
        byteStream.write(',');
        if (filter.getColumnOffset() != null) {
            byteStream.write('\'');
            writeBytesWithEscape(byteStream, filter.getColumnOffset());
            byteStream.write('\'');
        } else {
            byteStream.write(Long.toString(filter.getOffset()).getBytes());
        }
        byteStream.write(')');
    }

    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             ColumnPrefixFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write("('".getBytes());
        writeBytesWithEscape(byteStream, filter.getPrefix());
        byteStream.write("')".getBytes());
    }

    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             FirstKeyOnlyFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        byteStream.write(')');
    }

    private static void toParseableByteArray(ByteArrayOutputStream byteStream, KeyOnlyFilter filter) throws IOException {
        boolean lenAsVal;
        try {
            Field field = filter.getClass().getDeclaredField("lenAsVal");
            field.setAccessible(true);
            lenAsVal = (boolean)field.get(filter);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        byteStream.write(Boolean.toString(lenAsVal).getBytes());
        byteStream.write(')');
    }

    // FuzzyRowFilter('abc','101','ddd','010');
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, FuzzyRowFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');

        List<Pair<byte[], byte[]>> fuzzyKeysData;
        try {
            Field field = filter.getClass().getDeclaredField("fuzzyKeysData");
            field.setAccessible(true);
            fuzzyKeysData = (List<Pair<byte[], byte[]>>)field.get(filter);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < fuzzyKeysData.size(); i ++) {
            Pair<byte[], byte[]> data = fuzzyKeysData.get(i);
            byteStream.write("'".getBytes());
            writeBytesWithEscape(byteStream, data.getFirst());
            byteStream.write("'".getBytes());
            byteStream.write(',');
            byteStream.write("'".getBytes());
            writeBytesWithEscape(byteStream, data.getSecond());
            byteStream.write("'".getBytes());
            if (i < fuzzyKeysData.size() - 1) {
                byteStream.write(',');
            }
        }
        byteStream.write(')');
    }

    private static void toParseableByteArray(ByteArrayOutputStream byteStream, TimestampsFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        List<Long> timestamps = filter.getTimestamps();
        boolean canHint;
        try {
            Field field = filter.getClass().getDeclaredField("canHint");
            field.setAccessible(true);
            canHint = (boolean)field.get(filter);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < timestamps.size(); i ++) {
            Long timestamp = timestamps.get(i);
            byteStream.write(Long.toString(timestamp).getBytes());
            byteStream.write(',');
        }
        byteStream.write(Boolean.toString(canHint).getBytes());
        byteStream.write(')');
    }

    // MultiRowRangeFilter('a',true,'b',false,'c',true,'d',false);
    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             MultiRowRangeFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');

        List<MultiRowRangeFilter.RowRange> ranges = filter.getRowRanges();
        for (int i = 0; i < ranges.size(); i++) {
            MultiRowRangeFilter.RowRange range = ranges.get(i);
            byteStream.write("'".getBytes());
            writeBytesWithEscape(byteStream, range.getStartRow());
            byteStream.write("',".getBytes());
            byteStream.write(Boolean.toString(range.isStartRowInclusive()).getBytes());
            byteStream.write(',');

            byteStream.write("'".getBytes());
            writeBytesWithEscape(byteStream, range.getStopRow());
            byteStream.write("',".getBytes());
            byteStream.write(Boolean.toString(range.isStopRowInclusive()).getBytes());
            if (i < ranges.size() - 1) {
                byteStream.write(',');
            }
        }
        byteStream.write(')');
    }

    // InclusiveStopFilter('aaa');
    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             InclusiveStopFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        byteStream.write('\'');
        writeBytesWithEscape(byteStream, filter.getStopRowKey());
        byteStream.write('\'');
        byteStream.write(')');
    }

    // ColumnRangeFilter('a',true,'b',false);
    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             ColumnRangeFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');

        byteStream.write("'".getBytes());
        writeBytesWithEscape(byteStream, filter.getMinColumn());
        byteStream.write("',".getBytes());
        byteStream.write(Boolean.toString(filter.getMinColumnInclusive()).getBytes());
        byteStream.write(',');

        byteStream.write("'".getBytes());
        writeBytesWithEscape(byteStream, filter.getMaxColumn());
        byteStream.write("',".getBytes());
        byteStream.write(Boolean.toString(filter.getMaxColumnInclusive()).getBytes());
        byteStream.write(')');
    }

    // MultipleColumnPrefixFilter('a','b','d');
    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             MultipleColumnPrefixFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');

        byte[][] ranges = filter.getPrefix();
        for (int i = 0; i < ranges.length; i++) {
            byte[] range = ranges[i];
            byteStream.write("'".getBytes());
            writeBytesWithEscape(byteStream, range);
            byteStream.write("'".getBytes());
            if (i < ranges.length - 1) {
                byteStream.write(',');
            }
        }
        byteStream.write(')');
    }

    // ColumnCountGetFilter(100)
    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             ColumnCountGetFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        byteStream.write(Long.toString(filter.getLimit()).getBytes());
        byteStream.write(')');
    }

    // FirstKeyValueMatchingQualifiersFilter('q1','q2')
    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             FirstKeyValueMatchingQualifiersFilter filter) throws IOException {
        Set<byte[]> qualifiers;
        try {
            Field field = filter.getClass().getDeclaredField("qualifiers");
            field.setAccessible(true);
            qualifiers = (Set<byte[]>)field.get(filter);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        int i = 0;
        for (byte[] qualifier: qualifiers) {
            byteStream.write("'".getBytes());
            writeBytesWithEscape(byteStream, qualifier);
            byteStream.write("'".getBytes());
            if (i < qualifiers.size() - 1) {
                byteStream.write(',');
            }
            i++;
        }
        byteStream.write(')');
    }

    // PrefixFilter('prefix');
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, PrefixFilter filter)
                                                                                                   throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write("('".getBytes());
        writeBytesWithEscape(byteStream, filter.getPrefix());
        byteStream.write("')".getBytes());
    }

    // (SKIP filter)
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, SkipFilter filter)
                                                                                                 throws IOException {
        byteStream.write('(');
        byteStream.write(ParseConstants.SKIP_ARRAY);
        byteStream.write(' ');
        toParseableByteArray(byteStream, filter.getFilter());
        byteStream.write(')');
    }

    // (WHILE filter)
    private static void toParseableByteArray(ByteArrayOutputStream byteStream,
                                             WhileMatchFilter filter) throws IOException {
        byteStream.write('(');
        byteStream.write(ParseConstants.WHILE_ARRAY);
        byteStream.write(' ');
        toParseableByteArray(byteStream, filter.getFilter());
        byteStream.write(')');
    }

    // (filter and filter ...) or (filter or filter ...)
    // when filter list is empty, "" is generated, and empty filter list member is removed
    // in result parseable byteArray
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, FilterList filterList)
                                                                                                     throws IOException {
        List<Filter> filters = filterList.getFilters();
        boolean isEmpty = true;
        ByteArrayOutputStream oneFilterBytes = new ByteArrayOutputStream();
        for (int i = 0; i < filters.size(); i++) {
            toParseableByteArray(oneFilterBytes, filters.get(i));
            if (oneFilterBytes.size() == 0) {
                continue;
            }
            if (isEmpty) {
                byteStream.write('(');
                isEmpty = false;
            } else {
                byteStream.write(' ');
                if (filterList.getOperator().equals(FilterList.Operator.MUST_PASS_ALL)) {
                    byteStream.write(ParseConstants.AND);
                } else if (filterList.getOperator().equals(FilterList.Operator.MUST_PASS_ONE)) {
                    byteStream.write(ParseConstants.OR);
                } else {
                    throw new IllegalArgumentException("Invalid FilterList: " + filterList);
                }
                byteStream.write(' ');
            }
            oneFilterBytes.writeTo(byteStream);
            oneFilterBytes.reset();
        }
        if (!isEmpty) {
            byteStream.write(')');
        }
    }

    // when write family/qualifier/value/row into hbase filter, need add escape for
    // special character to prevent parse error in server
    public static void writeBytesWithEscape(ByteArrayOutputStream byteStream, byte[] bytes)
                                                                                           throws IOException {
        if (bytes == null) {
            return;
        }
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == '\'') {
                byteStream.write('\'');
            }
            byteStream.write(bytes[i]);
        }
    }
}
