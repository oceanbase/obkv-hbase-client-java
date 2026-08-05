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

import com.alipay.oceanbase.hbase.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.hbase.metrics.MetricsImporter;
import com.alipay.oceanbase.hbase.metrics.OHMetrics;
import com.alipay.oceanbase.hbase.util.OHBaseFuncUtils;
import com.alipay.oceanbase.hbase.util.TableHBaseLoggerFactory;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.OHOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.AbstractQueryStreamResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHBaseCellBatch;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHBaseCellRow;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryAsyncStreamResult;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryStreamResult;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import java.io.IOException;
import java.util.*;

import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_SCAN_LIGHTWEIGHT_RESULT_CELL_DEFAULT;
import static com.alipay.oceanbase.hbase.util.TableHBaseLoggerFactory.LCD;

@InterfaceAudience.Private
public class ClientStreamScanner extends AbstractClientScanner {

    private static final Logger             logger       = TableHBaseLoggerFactory
                                                             .getLogger(ClientStreamScanner.class);

    private final AbstractQueryStreamResult streamResult;

    private final String                    tableName;

    private byte[]                          family;

    private boolean                         closed       = false;

    private boolean                         streamNext   = true;

    private boolean                         isTableGroup = false;

    private final boolean                   lightweightResultCellEnabled;

    private OHMetrics                       metrics;

    public ClientStreamScanner(ObTableClientQueryStreamResult streamResult, String tableName,
                               byte[] family, boolean isTableGroup, OHMetrics metrics) {
        this(streamResult, tableName, family, isTableGroup, metrics,
            HBASE_HTABLE_SCAN_LIGHTWEIGHT_RESULT_CELL_DEFAULT);
    }

    public ClientStreamScanner(ObTableClientQueryStreamResult streamResult, String tableName,
                               byte[] family, boolean isTableGroup, OHMetrics metrics,
                               boolean lightweightResultCellEnabled) {
        this.streamResult = streamResult;
        this.tableName = tableName;
        this.family = family;
        this.isTableGroup = isTableGroup;
        this.metrics = metrics;
        this.lightweightResultCellEnabled = lightweightResultCellEnabled;
    }

    public ClientStreamScanner(ObTableClientQueryAsyncStreamResult streamResult, String tableName,
                               byte[] family, boolean isTableGroup, OHMetrics metrics) {
        this(streamResult, tableName, family, isTableGroup, metrics,
            HBASE_HTABLE_SCAN_LIGHTWEIGHT_RESULT_CELL_DEFAULT);
    }

    public ClientStreamScanner(ObTableClientQueryAsyncStreamResult streamResult, String tableName,
                               byte[] family, boolean isTableGroup, OHMetrics metrics,
                               boolean lightweightResultCellEnabled) {
        this.streamResult = streamResult;
        this.tableName = tableName;
        this.family = family;
        this.isTableGroup = isTableGroup;
        this.metrics = metrics;
        this.lightweightResultCellEnabled = lightweightResultCellEnabled;
    }

    @Override
    public Result next() throws IOException {
        long startTimeMs = System.currentTimeMillis();
        MetricsImporter importer = metrics == null ? null : new MetricsImporter();
        try {
            checkStatus();
            if (!streamResult.next()) {
                return null;
            }
            if (streamResult.isCurrentHBaseCell()) {
                return buildCompactResult(streamResult.drainCurrentHBaseRow());
            }
            return buildLegacyResult(streamResult.getRow());
        } catch (Exception e) {
            throw new IOException(String.format("get table %s stream next result error ",
                streamResult.getTableName()), e);
        } finally {
            if (metrics != null) {
                long duration = System.currentTimeMillis() - startTimeMs;
                importer.setDuration(duration);
                importer.setBatchSize(1);
                metrics.update(new ObPair<OHOperationType, MetricsImporter>(OHOperationType.SCAN,
                    importer));
            }
        }
    }

    private Result buildCompactResult(ObHBaseCellRow hbaseRow) {
        List<Cell> cells = new ArrayList<Cell>(hbaseRow.getCellCount());
        byte[] rowKey = hbaseRow.getRowKey();
        for (int sliceIndex = 0; sliceIndex < hbaseRow.getSliceCount(); sliceIndex++) {
            ObHBaseCellBatch batch = hbaseRow.getBatch(sliceIndex);
            int toIndex = hbaseRow.getToIndex(sliceIndex);
            for (int index = hbaseRow.getFromIndex(sliceIndex); index < toIndex; index++) {
                addCompactResultCell(cells, rowKey, batch.getQualifier(index),
                    batch.getTimestamp(index), batch.getValue(index));
            }
        }
        OHBaseFuncUtils.sortHBaseResult(cells);
        return createCompactResult(cells);
    }

    private void addCompactResultCell(List<Cell> cells, byte[] rowKey, byte[] qualifier,
                                      long timestamp, byte[] value) {
        if (lightweightResultCellEnabled) {
            if (isTableGroup) {
                cells.add(OHBaseResultCell.createTableGroup(rowKey, qualifier, timestamp, value));
            } else {
                cells.add(OHBaseResultCell.create(rowKey, family, qualifier, timestamp, value));
            }
            return;
        }
        if (isTableGroup) {
            cells
                .add(OHBaseFuncUtils.createTableGroupKeyValue(rowKey, qualifier, timestamp, value));
        } else {
            cells.add(new KeyValue(rowKey, family, qualifier, timestamp, value));
        }
    }

    @SuppressWarnings("unchecked")
    private Result createCompactResult(List<Cell> cells) {
        if (lightweightResultCellEnabled) {
            return Result.create(cells);
        }
        return new Result((List<KeyValue>) (List<?>) cells);
    }

    private Result buildLegacyResult(List<ObObj> startRow) throws Exception {
        byte[][] familyAndQualifier = new byte[2][];
            if (this.isTableGroup) {
                // split family and qualifier
                familyAndQualifier = OHBaseFuncUtils.extractFamilyFromQualifier((byte[]) startRow
                    .get(1).getValue());
                this.family = familyAndQualifier[0];
            } else {
                familyAndQualifier[1] = (byte[]) startRow.get(1).getValue();
            }

            byte[] sk = (byte[]) startRow.get(0).getValue();
            byte[] sq = familyAndQualifier[1];
            long st = (Long) startRow.get(2).getValue();
            byte[] sv = (byte[]) startRow.get(3).getValue();

            KeyValue startKeyValue = new KeyValue(sk, family, sq, st, sv);
            List<KeyValue> keyValues = new ArrayList<KeyValue>();
            keyValues.add(startKeyValue);
            while (!streamResult.getCacheRows().isEmpty() && streamResult.next()) {
                List<ObObj> row = streamResult.getRow();
                if (this.isTableGroup) {
                    // split family and qualifier
                    familyAndQualifier = OHBaseFuncUtils.extractFamilyFromQualifier((byte[]) row
                        .get(1).getValue());
                    this.family = familyAndQualifier[0];
                } else {
                    familyAndQualifier[1] = (byte[]) row.get(1).getValue();
                }
                byte[] k = (byte[]) row.get(0).getValue();
                byte[] q = familyAndQualifier[1];
                long t = (Long) row.get(2).getValue();
                byte[] v = (byte[]) row.get(3).getValue();
                if (Arrays.equals(sk, k)) {
                    // when rowKey is equal to the previous rowKey ,merge the result into the same result
                    keyValues.add(new KeyValue(k, family, q, t, v));
                } else {
                    streamResult.getCacheRows().addFirst(row);
                    break;
                }
            }
        OHBaseFuncUtils.sortHBaseResult(keyValues);
            return new Result(keyValues);
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
        ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
        for (int i = 0; i < nbRows; i++) {
            Result next = next();
            if (next != null) {
                resultSets.add(next);
            } else {
                break;
            }
        }
        return resultSets.toArray(new Result[resultSets.size()]);
    }

    @Override
    public boolean renewLease() {
        throw new FeatureNotSupportedException("not supported yet'");
    }

    private void checkStatus() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("table " + tableName + " family "
                                            + Bytes.toString(family) + " scanner is  closed");
        }
    }

    @Override
    public void close() {
        try {
            closed = true;
            streamResult.close();
        } catch (Exception e) {
            logger.error(LCD.convert("01-00001"), streamResult.getTableName(), e);
        }
    }
}
