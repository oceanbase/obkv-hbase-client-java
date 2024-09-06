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

import org.apache.hadoop.hbase.filter.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class HBaseFilterUtils {

    public static byte[] toParseableByteArray(Filter filter) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        toParseableByteArray(byteStream, filter);
        return byteStream.toByteArray();
    }

    private static void toParseableByteArray(ByteArrayOutputStream byteStream, Filter filter) throws IOException {
        if (filter == null) {
            throw new IllegalArgumentException("Filter is null");
        } else if (filter instanceof CompareFilter) {
            // RowFilter, ValueFilter, QualifierFilter
            toParseableByteArray(byteStream, (CompareFilter) filter);
        } else if (filter instanceof SingleColumnValueFilter) {
            toParseableByteArray(byteStream, (SingleColumnValueFilter) filter);
        } else if (filter instanceof PageFilter) {
            toParseableByteArray(byteStream, (PageFilter) filter);
        } else if (filter instanceof ColumnCountGetFilter) {
            toParseableByteArray(byteStream, (ColumnCountGetFilter) filter);
        } else if (filter instanceof PrefixFilter) {
            toParseableByteArray(byteStream, (PrefixFilter) filter);
        } else if (filter instanceof FilterList) {
            toParseableByteArray(byteStream, (FilterList) filter);
        } else if (filter instanceof ColumnPaginationFilter) {
            toParseableByteArray(byteStream, (ColumnPaginationFilter) filter);
        } else if (filter instanceof SkipFilter) {
            toParseableByteArray(byteStream, (SkipFilter) filter);
        } else if (filter instanceof WhileMatchFilter) {
            toParseableByteArray(byteStream, (WhileMatchFilter) filter);
        } else {
            throw new IllegalArgumentException("Invalid filter: " + filter);
        }
    }

    private static byte[] toParseableByteArray(CompareFilter.CompareOp op) {
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

    private static void toParseableByteArray(ByteArrayOutputStream byteStream, ByteArrayComparable comparator) throws IOException {
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
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, CompareFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        byteStream.write(toParseableByteArray(filter.getOperator()));
        byteStream.write(',');
        toParseableByteArray(byteStream, filter.getComparator());
        byteStream.write(')');
    }

    // SingleColumnValueFilter('cf1','col1',=,'binary:123',true,true)
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, SingleColumnValueFilter filter) throws IOException {
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
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, PageFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        byteStream.write(Long.toString(filter.getPageSize()).getBytes());
        byteStream.write(')');
    }

    // ColumnPaginationFilter(ColumnPaginationFilter(10,2)
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, ColumnPaginationFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        byteStream.write(Long.toString(filter.getLimit()).getBytes());
        byteStream.write(',');
        byteStream.write(Long.toString(filter.getOffset()).getBytes());
        byteStream .write(')');
    }

    // ColumnCountGetFilter(100)
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, ColumnCountGetFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write('(');
        byteStream.write(Long.toString(filter.getLimit()).getBytes());
        byteStream .write(')');
    }

    // PrefixFilter('prefix');
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, PrefixFilter filter) throws IOException {
        byteStream.write(filter.getClass().getSimpleName().getBytes());
        byteStream.write("('".getBytes());
        writeBytesWithEscape(byteStream, filter.getPrefix());
        byteStream .write("')".getBytes());
    }

    // (SKIP filter)
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, SkipFilter filter) throws IOException {
        byteStream.write('(');
        byteStream.write(ParseConstants.SKIP_ARRAY);
        byteStream.write(' ');
        toParseableByteArray(byteStream, filter.getFilter());
        byteStream.write(')');
    }

    // (WHILE filter)
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, WhileMatchFilter filter) throws IOException {
        byteStream.write('(');
        byteStream.write(ParseConstants.WHILE_ARRAY);
        byteStream.write(' ');
        toParseableByteArray(byteStream, filter.getFilter());
        byteStream.write(')');
    }

    // (filter and filter ...) or (filter or filter ...)
    // when filter list is empty, "" is generated, and empty filter list member is removed
    // in result parseable byteArray
    private static void toParseableByteArray(ByteArrayOutputStream byteStream, FilterList filterList) throws IOException {
        List<Filter> filters = filterList.getFilters();
        boolean isEmpty = true;
        ByteArrayOutputStream oneFilterBytes = new ByteArrayOutputStream();
        for (int i = 0; i < filters.size(); i++) {
            toParseableByteArray(oneFilterBytes, filters.get(i));
            if (oneFilterBytes.size() == 0) { continue; }
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
    public static void writeBytesWithEscape(ByteArrayOutputStream byteStream, byte[] bytes) throws IOException {
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
