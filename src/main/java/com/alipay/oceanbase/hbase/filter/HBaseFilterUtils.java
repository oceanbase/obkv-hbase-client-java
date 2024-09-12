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
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

@InterfaceAudience.Private
public class HBaseFilterUtils {

    public static String toParseableString(Filter filter) {
        if (filter == null) {
            throw new IllegalArgumentException("Filter is null");
        } else if (filter instanceof CompareFilter) {
            // RowFilter, ValueFilter, QualifierFilter
            return toParseableString((CompareFilter) filter);
        } else if (filter instanceof SingleColumnValueFilter) {
            return toParseableString((SingleColumnValueFilter) filter);
        } else if (filter instanceof PageFilter) {
            return toParseableString((PageFilter) filter);
        } else if (filter instanceof ColumnCountGetFilter) {
            return toParseableString((ColumnCountGetFilter) filter);
        } else if (filter instanceof PrefixFilter) {
            return toParseableString((PrefixFilter) filter);
        } else if (filter instanceof FilterList) {
            return toParseableString((FilterList) filter);
        } else if (filter instanceof RandomRowFilter) {
            return toParseableString((RandomRowFilter) filter);
        } else if (filter instanceof ColumnPaginationFilter) {
            return toParseableString((ColumnPaginationFilter) filter);
        } else if (filter instanceof ColumnPrefixFilter) {
            return toParseableString((ColumnPrefixFilter) filter);
        } else if (filter instanceof SkipFilter) {
            return toParseableString((SkipFilter) filter);
        } else if (filter instanceof WhileMatchFilter) {
            return toParseableString((WhileMatchFilter) filter);
        } else {
            throw new IllegalArgumentException("Invalid filter: " + filter);
        }
    }

    public static String toParseableString(CompareFilter.CompareOp op) {
        if (op == null) {
            throw new IllegalArgumentException("Compare operator is null");
        }
        switch (op) {
            case LESS:
                return Bytes.toString(ParseConstants.LESS_THAN_ARRAY);
            case LESS_OR_EQUAL:
                return Bytes.toString(ParseConstants.LESS_THAN_OR_EQUAL_TO_ARRAY);
            case EQUAL:
                return Bytes.toString(ParseConstants.EQUAL_TO_ARRAY);
            case NOT_EQUAL:
                return Bytes.toString(ParseConstants.NOT_EQUAL_TO_ARRAY);
            case GREATER_OR_EQUAL:
                return Bytes.toString(ParseConstants.GREATER_THAN_OR_EQUAL_TO_ARRAY);
            case GREATER:
                return Bytes.toString(ParseConstants.GREATER_THAN_ARRAY);
            default:
                throw new IllegalArgumentException("Invalid compare operator: " + op);
        }
    }

    private static String toParseableString(ByteArrayComparable comparator) {
        if (comparator == null) {
            throw new IllegalArgumentException("Comparator is null");
        }
        StringBuilder sb = new StringBuilder();
        if (comparator instanceof BinaryComparator) {
            sb.append('\'').append(Bytes.toString(ParseConstants.binaryType)).append(':')
                .append(Bytes.toString(comparator.getValue())).append('\'');
        } else if (comparator instanceof BinaryPrefixComparator) {
            sb.append('\'').append(Bytes.toString(ParseConstants.binaryPrefixType)).append(':')
                .append(Bytes.toString(comparator.getValue())).append('\'');
        } else if (comparator instanceof RegexStringComparator) {
            sb.append('\'').append(Bytes.toString(ParseConstants.regexStringType)).append(':')
                .append(Bytes.toString(comparator.getValue())).append('\'');
        } else if (comparator instanceof SubstringComparator) {
            sb.append('\'').append(Bytes.toString(ParseConstants.substringType)).append(':')
                .append(Bytes.toString(comparator.getValue())).append('\'');
        } else {
            throw new IllegalArgumentException("This comparator has not been implemented "
                                               + comparator);
        }
        return sb.toString();
    }

    private static String toParseableString(CompareFilter filter) {
        return filter.getClass().getSimpleName() + '(' + toParseableString(filter.getOperator())
               + ',' + toParseableString(filter.getComparator()) + ')';
    }

    private static String toParseableString(SingleColumnValueFilter filter) {
        return filter.getClass().getSimpleName() + "('" + Bytes.toString(filter.getFamily())
               + "','" + Bytes.toString(filter.getQualifier()) + "',"
               + toParseableString(filter.getOperator()) + ','
               + toParseableString(filter.getComparator()) + ',' + filter.getFilterIfMissing()
               + ',' + filter.getLatestVersionOnly() + ')';
    }

    private static String toParseableString(PageFilter filter) {
        return filter.getClass().getSimpleName() + '(' + filter.getPageSize() + ')';
    }

    private static String toParseableString(RandomRowFilter filter) {
        return filter.getClass().getSimpleName() + "(" + Bytes.toInt(Bytes.toBytes(filter.getChance())) + ")";
    }

    private static String toParseableString(ColumnPaginationFilter filter) {
        if (filter.getColumnOffset() != null) {
            return filter.getClass().getSimpleName() + '(' + filter.getLimit() + ",'"
                   + Bytes.toString(filter.getColumnOffset()) + "')";
        } else {
            return filter.getClass().getSimpleName() + '(' + filter.getLimit() + ','
                   + filter.getOffset() + ')';
        }
    }

    private static String toParseableString(ColumnPrefixFilter filter) {
        return filter.getClass().getSimpleName() + "('" + Bytes.toString(filter.getPrefix()) + "')";
    }

    private static String toParseableString(ColumnCountGetFilter filter) {
        return filter.getClass().getSimpleName() + '(' + filter.getLimit() + ')';
    }

    private static String toParseableString(PrefixFilter filter) {
        return filter.getClass().getSimpleName() + "('" + Bytes.toString(filter.getPrefix()) + "')";
    }

    private static String toParseableString(SkipFilter filter) {
        return "(" + Bytes.toString(ParseConstants.SKIP_ARRAY) + " "
               + toParseableString(filter.getFilter()) + ")";
    }

    private static String toParseableString(WhileMatchFilter filter) {
        return "(" + Bytes.toString(ParseConstants.WHILE_ARRAY) + " "
               + toParseableString(filter.getFilter()) + ")";
    }

    private static String toParseableString(FilterList filterList) {
        StringBuilder sb = new StringBuilder();
        List<Filter> filters = filterList.getFilters();
        boolean isEmpty = true;
        for (int i = 0; i < filters.size(); i++) {
            String filterString = toParseableString(filters.get(i));
            if (filterString.isEmpty())
                continue;
            if (isEmpty) {
                sb.append("(").append(filterString);
                isEmpty = false;
            } else {
                sb.append(" ");
                if (filterList.getOperator().equals(FilterList.Operator.MUST_PASS_ALL)) {
                    sb.append(Bytes.toString(ParseConstants.AND));
                } else if (filterList.getOperator().equals(FilterList.Operator.MUST_PASS_ONE)) {
                    sb.append(Bytes.toString(ParseConstants.OR));
                } else {
                    throw new IllegalArgumentException("Invalid FilterList: " + filterList);
                }
                sb.append(" ").append(filterString);
            }
        }
        if (!isEmpty) {
            sb.append(")");
        }
        return sb.toString();
    }

}
