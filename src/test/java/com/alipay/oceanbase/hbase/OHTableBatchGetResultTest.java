/*-
 * #%L
 * com.oceanbase:obkv-hbase-client
 * %%
 * Copyright (C) 2022 - 2026 OceanBase Group
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

import com.alipay.oceanbase.hbase.util.BatchError;
import com.alipay.oceanbase.hbase.result.OHBaseResultCell;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableSingleOpEntity;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableSingleOpResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHBaseCellBatch;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class OHTableBatchGetResultTest {
    private ExecutorService executor;
    private OHTable         table;

    @Before
    public void setUp() {
        executor = Executors.newSingleThreadExecutor();
        table = new OHTable(Bytes.toBytes("test"), mock(ObTableClient.class), executor);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void convertsKqtvAndPreservesRequestOrder() throws Exception {
        List<Row> actions = Arrays.<Row> asList(new Get(Bytes.toBytes("r1")),
            new Get(Bytes.toBytes("r2")));
        List<Object> raw = Arrays.<Object> asList(wrappedResult("r1", "cf\0q1", 2),
            wrappedResult("r2", "cf\0q2", 1));
        Object[] results = new Object[2];

        table.consumePureGetBatchResults(actions, results, raw, new BatchError());

        assertEquals(2, ((Result) results[0]).size());
        assertEquals(1, ((Result) results[1]).size());
        assertEquals("r1", Bytes.toString(((Result) results[0]).getRow()));
        assertEquals("r2", Bytes.toString(((Result) results[1]).getRow()));
        assertTrue(((Result) results[0]).rawCells()[0] instanceof OHBaseResultCell);
    }

    @Test
    public void mapsSingleMissingResultToEmptyResult() throws Exception {
        Object[] results = new Object[1];
        table.consumePureGetBatchResults(
            Collections.<Row> singletonList(new Get(Bytes.toBytes("missing"))), results,
            Collections.emptyList(), new BatchError());
        assertTrue(((Result) results[0]).isEmpty());
    }

    @Test(expected = IOException.class)
    public void rejectsMalformedKqtvResult() throws Exception {
        ObTableSingleOpResult result = new ObTableSingleOpResult();
        result.setEntity(ObTableSingleOpEntity.getInstance(null, null,
            new String[] { "K", "Q", "T" },
            new Object[] { Bytes.toBytes("r"), Bytes.toBytes("cf\0q"), 1L }));
        table.generateGetResult(result);
    }

    @Test
    public void consumesCompactKqtvBatch() throws Exception {
        ObHBaseCellBatch batch = new ObHBaseCellBatch(2);
        batch.setCell(0, Bytes.toBytes("r1"), Bytes.toBytes("cf\0q1"), 100L, Bytes.toBytes("v0"));
        batch.setCell(1, Bytes.toBytes("r1"), Bytes.toBytes("cf\0q1"), 99L, Bytes.toBytes("v1"));
        ObTableSingleOpEntity entity = new ObTableSingleOpEntity();
        setCompactBatch(entity, batch);
        ObTableSingleOpResult result = new ObTableSingleOpResult();
        result.setEntity(entity);

        List<Cell> cells = table.generateGetResult(result);

        assertEquals(2, cells.size());
        assertEquals("r1", Bytes.toString(cells.get(0).getRowArray(), cells.get(0).getRowOffset(),
            cells.get(0).getRowLength()));
        assertEquals(99L, cells.get(1).getTimestamp());
        assertTrue(cells.get(0) instanceof OHBaseResultCell);
    }

    private static MutationResult wrappedResult(String row, String qualifier, int versions) {
        String[] names = new String[versions * 4];
        Object[] values = new Object[versions * 4];
        for (int i = 0; i < versions; i++) {
            int offset = i * 4;
            names[offset] = "K";
            names[offset + 1] = "Q";
            names[offset + 2] = "T";
            names[offset + 3] = "V";
            values[offset] = Bytes.toBytes(row);
            values[offset + 1] = Bytes.toBytes(qualifier);
            values[offset + 2] = 100L - i;
            values[offset + 3] = Bytes.toBytes("v" + i);
        }
        ObTableSingleOpResult result = new ObTableSingleOpResult();
        result.setEntity(ObTableSingleOpEntity.getInstance(null, null, names, values));
        return new MutationResult(result);
    }

    private static void setCompactBatch(ObTableSingleOpEntity entity, ObHBaseCellBatch batch)
                                                                                             throws Exception {
        java.lang.reflect.Field field = ObTableSingleOpEntity.class
            .getDeclaredField("hbaseCellBatch");
        field.setAccessible(true);
        field.set(entity, batch);
    }
}
