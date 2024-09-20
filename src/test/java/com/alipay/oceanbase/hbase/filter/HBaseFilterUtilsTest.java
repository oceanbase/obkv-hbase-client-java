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
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

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
            Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));

            filter = new RowFilter(ops[i], new BinaryPrefixComparator("testRowFilter".getBytes()));
            expect = String.format("RowFilter(%s,'binaryprefix:testRowFilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));

            filter = new RowFilter(ops[i], new RegexStringComparator("testRowFilter"));
            expect = String.format("RowFilter(%s,'regexstring:testRowFilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));

            filter = new RowFilter(ops[i], new SubstringComparator("testRowFilter"));
            expect = String.format("RowFilter(%s,'substring:testrowfilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));
        }
    }

    @Test
    public void testValueFilter() throws IOException {
        for (int i = 0; i < ops.length; i++) {
            ValueFilter filter = new ValueFilter(ops[i], new BinaryComparator(
                "testValueFilter".getBytes()));
            String expect = String.format("ValueFilter(%s,'binary:testValueFilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));

            filter = new ValueFilter(ops[i], new BinaryPrefixComparator(
                "testValueFilter".getBytes()));
            expect = String.format("ValueFilter(%s,'binaryprefix:testValueFilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));

            filter = new ValueFilter(ops[i], new RegexStringComparator("testValueFilter"));
            expect = String.format("ValueFilter(%s,'regexstring:testValueFilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));

            filter = new ValueFilter(ops[i], new SubstringComparator("testValueFilter"));
            expect = String.format("ValueFilter(%s,'substring:testvaluefilter')", opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));
        }
    }

    @Test
    public void testQualifierFilter() throws IOException {
        for (int i = 0; i < ops.length; i++) {
            QualifierFilter filter = new QualifierFilter(ops[i], new BinaryComparator(
                "testQualifierFilter".getBytes()));
            String expect = String.format("QualifierFilter(%s,'binary:testQualifierFilter')",
                opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));

            filter = new QualifierFilter(ops[i], new BinaryPrefixComparator(
                "testQualifierFilter".getBytes()));
            expect = String.format("QualifierFilter(%s,'binaryprefix:testQualifierFilter')",
                opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));

            filter = new QualifierFilter(ops[i], new RegexStringComparator("testQualifierFilter"));
            expect = String.format("QualifierFilter(%s,'regexstring:testQualifierFilter')",
                opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));

            filter = new QualifierFilter(ops[i], new SubstringComparator("testQualifierFilter"));
            expect = String.format("QualifierFilter(%s,'substring:testqualifierfilter')",
                opFlags[i]);
            Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));
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
            Assert.assertArrayEquals(expect.getBytes(), HBaseFilterUtils.toParseableByteArray(filter));
        }
    }

    @Test
    public void testPageFilter() throws IOException {
        PageFilter filter = new PageFilter(128);
        Assert.assertArrayEquals("PageFilter(128)".getBytes(), HBaseFilterUtils.toParseableByteArray(filter));
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
    public void testColumnCountGetFilter() throws IOException {
        ColumnCountGetFilter filter = new ColumnCountGetFilter(513);
        Assert.assertArrayEquals("ColumnCountGetFilter(513)".getBytes(), HBaseFilterUtils.toParseableByteArray(filter));
    }

    @Test
    public void testPrefixFilter() throws IOException {
        PrefixFilter filter = new PrefixFilter("prefix".getBytes());
        System.out.println(new String(HBaseFilterUtils.toParseableByteArray(filter)));
        Assert.assertArrayEquals("PrefixFilter('prefix')".getBytes(), HBaseFilterUtils.toParseableByteArray(filter));
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
        Assert.assertArrayEquals("(WHILE QualifierFilter(>,'binaryprefix:whileMatchFilter'))".getBytes(),
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
        Assert.assertArrayEquals(
                ("(RowFilter(=,'binary:testSkipFilter') "
                        + "AND QualifierFilter(>,'binaryprefix:whileMatchFilter') AND (SKIP PageFilter(128)) AND ColumnPaginationFilter(2,2))").getBytes(),
                HBaseFilterUtils.toParseableByteArray(filterList));

        filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(rowFilter);
        filterList.addFilter(qualifierFilter);
        filterList.addFilter(columnPaginationFilter);

        Assert.assertArrayEquals(
                    ("(RowFilter(=,'binary:testSkipFilter') "
                        + "OR QualifierFilter(>,'binaryprefix:whileMatchFilter') OR ColumnPaginationFilter(2,2))").getBytes(),
                HBaseFilterUtils.toParseableByteArray(filterList));
    }
}
