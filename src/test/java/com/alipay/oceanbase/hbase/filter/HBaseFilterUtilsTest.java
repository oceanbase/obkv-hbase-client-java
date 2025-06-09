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

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class HBaseFilterUtilsTest {
    private static final CompareFilter.CompareOp[] ops     = { CompareFilter.CompareOp.LESS,
            CompareFilter.CompareOp.LESS_OR_EQUAL, CompareFilter.CompareOp.EQUAL,
            CompareFilter.CompareOp.NOT_EQUAL, CompareFilter.CompareOp.GREATER_OR_EQUAL,
            CompareFilter.CompareOp.GREATER               };
    private static final String[]                  opFlags = { "<", "<=", "=", "!=", ">=", ">" };

    @BeforeClass
    public static void beforeClass() {
        Assert.assertEquals(ops.length, opFlags.length);
    }

    @Test
    public void testRowFilter() throws IOException {
        for (int i = 0; i < ops.length; i++) {
            RowFilter filter = new RowFilter(ops[i], new BinaryComparator(
                "testRowFilter".getBytes()));
            String expect = String.format("RowFilter(%s,'binary:testRowFilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));

            filter = new RowFilter(ops[i], new BinaryPrefixComparator("testRowFilter".getBytes()));
            expect = String.format("RowFilter(%s,'binaryprefix:testRowFilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));

            filter = new RowFilter(ops[i], new RegexStringComparator("testRowFilter"));
            expect = String.format("RowFilter(%s,'regexstring:testRowFilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));

            filter = new RowFilter(ops[i], new SubstringComparator("testRowFilter"));
            expect = String.format("RowFilter(%s,'substring:testrowfilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));
        }
    }

    @Test
    public void testValueFilter() throws IOException {
        for (int i = 0; i < ops.length; i++) {
            ValueFilter filter = new ValueFilter(ops[i], new BinaryComparator(
                "testValueFilter".getBytes()));
            String expect = String.format("ValueFilter(%s,'binary:testValueFilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));

            filter = new ValueFilter(ops[i], new BinaryPrefixComparator(
                "testValueFilter".getBytes()));
            expect = String.format("ValueFilter(%s,'binaryprefix:testValueFilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));

            filter = new ValueFilter(ops[i], new RegexStringComparator("testValueFilter"));
            expect = String.format("ValueFilter(%s,'regexstring:testValueFilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));

            filter = new ValueFilter(ops[i], new SubstringComparator("testValueFilter"));
            expect = String.format("ValueFilter(%s,'substring:testvaluefilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));
        }
    }

    @Test
    public void testQualifierFilter() throws IOException {
        for (int i = 0; i < ops.length; i++) {
            QualifierFilter filter = new QualifierFilter(ops[i], new BinaryComparator(
                "testQualifierFilter".getBytes()));
            String expect = String.format("QualifierFilter(%s,'binary:testQualifierFilter')",
                opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));

            filter = new QualifierFilter(ops[i], new BinaryPrefixComparator(
                "testQualifierFilter".getBytes()));
            expect = String.format("QualifierFilter(%s,'binaryprefix:testQualifierFilter')",
                opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));

            filter = new QualifierFilter(ops[i], new RegexStringComparator("testQualifierFilter"));
            expect = String.format("QualifierFilter(%s,'regexstring:testQualifierFilter')",
                opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));

            filter = new QualifierFilter(ops[i], new SubstringComparator("testQualifierFilter"));
            expect = String.format("QualifierFilter(%s,'substring:testqualifierfilter')",
                opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));
        }
    }

    @Test
    public void testSingleColumnValueFilter() throws IOException {
        for (int i = 0; i < ops.length; i++) {
            String expect = String.format(
                "SingleColumnValueFilter('family','qualifier',%s,'binary:value',false,true)",
                opFlags[i]);
            SingleColumnValueFilter filter = new SingleColumnValueFilter("family".getBytes(),
                "qualifier".getBytes(), ops[i], "value".getBytes());
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));
        }
    }

    @Test
    public void testSingleColumnValueExcludeFilter() throws IOException {
        for (int i = 0; i < ops.length; i++) {
            String expect = String
                .format(
                    "SingleColumnValueExcludeFilter('family','qualifier',%s,'binary:value',false,true)",
                    opFlags[i]);
            SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(
                "family".getBytes(), "qualifier".getBytes(), ops[i], "value".getBytes());
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));
        }
    }

    @Test
    public void testDependentColumnFilter() throws IOException {
        DependentColumnFilter filter = new DependentColumnFilter("family".getBytes(),
            "qualifier".getBytes());
        String expect = "DependentColumnFilter('family','qualifier',false)";
        Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));
        filter = new DependentColumnFilter("family".getBytes(), "qualifier".getBytes(), true);
        expect = "DependentColumnFilter('family','qualifier',true)";
        Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));
        for (int i = 0; i < ops.length; ++i) {
            filter = new DependentColumnFilter("family".getBytes(), "qualifier".getBytes(), false,
                ops[i], new BinaryComparator("value".getBytes()));
            expect = String.format(
                "DependentColumnFilter('family','qualifier',false,%s,'binary:value')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));
        }
    }

    @Test
    public void testPageFilter() throws IOException {
        PageFilter filter = new PageFilter(128);
        Assert.assertArrayEquals("PageFilter(128)".getBytes(),
            HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testColumnPaginationFilter() throws IOException {
        ColumnPaginationFilter filter = new ColumnPaginationFilter(2, 2);
        Assert.assertArrayEquals("ColumnPaginationFilter(2,2)".getBytes(),
            HBaseFilterUtils.toParseableByteArray(filter));

        filter = new ColumnPaginationFilter(2, Bytes.toBytes("a"));
        Assert.assertArrayEquals("ColumnPaginationFilter(2,'a')".getBytes(),
            HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testColumnPrefixFilter() throws IOException {
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("pre"));
        Assert.assertArrayEquals("ColumnPrefixFilter('pre')".getBytes(),
            HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testFuzzyRowFilter() throws IOException {
        List<Pair<byte[], byte[]>> fuzzyKey = new ArrayList<>();
        fuzzyKey.add(new Pair<byte[], byte[]>(Bytes.toBytes("abc"), Bytes.toBytes("101")));
        fuzzyKey.add(new Pair<byte[], byte[]>(Bytes.toBytes("ddd"), Bytes.toBytes("010")));

        FuzzyRowFilter filter = new FuzzyRowFilter(fuzzyKey);
        System.out.println(Bytes.toString(HBaseFilterUtils.toParseableByteArray(filter)));
        Assert.assertArrayEquals("FuzzyRowFilter('abc','101','ddd','010')".getBytes(), HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testMultiRowRangeFilter() throws IOException {
        List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>();
        ranges.add(new MultiRowRangeFilter.RowRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), false));
        ranges.add(new MultiRowRangeFilter.RowRange(Bytes.toBytes("c"), true, Bytes.toBytes("d$%%"), false));

        MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
        System.out.println(Bytes.toString(HBaseFilterUtils.toParseableByteArray(filter)));
        Assert.assertArrayEquals("MultiRowRangeFilter('a',true,'b',false,'c',true,'d$%%',false)".getBytes(), HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testInclusiveStopFilter() throws IOException {
        InclusiveStopFilter filter = new InclusiveStopFilter(Bytes.toBytes("aaa"));
        Assert.assertArrayEquals("InclusiveStopFilter('aaa')".getBytes(),
            HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testColumnRangeFilter() throws IOException {
        ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes("a"), true,
            Bytes.toBytes("b"), false);
        Assert.assertArrayEquals("ColumnRangeFilter('a',true,'b',false)".getBytes(),
            HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testMultipleColumnPrefixFilter() throws IOException {
        byte[][] prefix = { Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("d"), };
        MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(prefix);
        Assert.assertArrayEquals("MultipleColumnPrefixFilter('a','b','d')".getBytes(),
            HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testFamilyFilter() throws IOException {
        FamilyFilter filter = new FamilyFilter(CompareFilter.CompareOp.NOT_EQUAL,
            new BinaryComparator(Bytes.toBytes("cf")));
        Assert.assertArrayEquals("FamilyFilter(!=,'binary:cf')".getBytes(),
            HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testColumnCountGetFilter() throws IOException {
        ColumnCountGetFilter filter = new ColumnCountGetFilter(513);
        Assert.assertArrayEquals("ColumnCountGetFilter(513)".getBytes(),
            HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testFirstKeyValueMatchingQualifiersFilter() throws IOException {
        TreeSet<byte []> qualifiers = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        qualifiers.add(Bytes.toBytes("q1"));
        qualifiers.add(Bytes.toBytes("q2"));
        FirstKeyValueMatchingQualifiersFilter filter = new FirstKeyValueMatchingQualifiersFilter(qualifiers);
        Assert.assertArrayEquals("FirstKeyValueMatchingQualifiersFilter('q1','q2')".getBytes(),
                HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testPrefixFilter() throws IOException {
        PrefixFilter filter = new PrefixFilter("prefix".getBytes());
        System.out.println(new String(HBaseFilterUtils.toParseableByteArray(filter)));
        Assert.assertArrayEquals("PrefixFilter('prefix')".getBytes(),
            HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testSkipFilter() throws IOException {
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            "testSkipFilter".getBytes()));
        SkipFilter filter = new SkipFilter(rowFilter);
        Assert.assertArrayEquals("(SKIP RowFilter(=,'binary:testSkipFilter'))".getBytes(),
            HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testWhileFilter() throws IOException {
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.GREATER,
            new BinaryPrefixComparator("whileMatchFilter".getBytes()));
        WhileMatchFilter filter = new WhileMatchFilter(qualifierFilter);
        Assert.assertArrayEquals(
            "(WHILE QualifierFilter(>,'binaryprefix:whileMatchFilter'))".getBytes(),
            HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testFilterList() throws IOException {
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
            "testSkipFilter".getBytes()));
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.GREATER,
            new BinaryPrefixComparator("whileMatchFilter".getBytes()));
        SkipFilter skipFilter = new SkipFilter(new PageFilter(128));
        ColumnPaginationFilter columnPaginationFilter = new ColumnPaginationFilter(2, 2);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(rowFilter);
        filterList.addFilter(qualifierFilter);
        filterList.addFilter(skipFilter);
        filterList.addFilter(columnPaginationFilter);

        System.out.println(new String(HBaseFilterUtils.toParseableByteArray(filterList)));
        Assert
            .assertArrayEquals(
                ("(RowFilter(=,'binary:testSkipFilter') "
                 + "AND QualifierFilter(>,'binaryprefix:whileMatchFilter') AND (SKIP PageFilter(128)) AND ColumnPaginationFilter(2,2))")
                    .getBytes(), HBaseFilterUtils.toParseableByteArray(filterList));

        filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(rowFilter);
        filterList.addFilter(qualifierFilter);
        filterList.addFilter(columnPaginationFilter);

        Assert
            .assertArrayEquals(
                ("(RowFilter(=,'binary:testSkipFilter') "
                 + "OR QualifierFilter(>,'binaryprefix:whileMatchFilter') OR ColumnPaginationFilter(2,2))")
                    .getBytes(), HBaseFilterUtils.toParseableByteArray(filterList));
    }
}
