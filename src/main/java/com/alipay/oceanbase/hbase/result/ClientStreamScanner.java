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

import com.alipay.oceanbase.hbase.util.OHBaseFuncUtils;
import com.alipay.oceanbase.hbase.util.TableHBaseLoggerFactory;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.AbstractQueryStreamResult;
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

import static com.alipay.oceanbase.hbase.util.TableHBaseLoggerFactory.LCD;

@InterfaceAudience.Private
public class ClientStreamScanner extends AbstractClientScanner {

    protected static final Logger             logger       = TableHBaseLoggerFactory
                                                             .getLogger(ClientStreamScanner.class);

    protected final AbstractQueryStreamResult streamResult;

    protected final String                    tableName;

    protected byte[]                          family;

    protected boolean                         closed       = false;

    protected boolean                         isTableGroup = false;

    public ClientStreamScanner(ObTableClientQueryStreamResult streamResult, String tableName,
                               byte[] family, boolean isTableGroup) {
        this.streamResult = streamResult;
        this.tableName = tableName;
        this.family = family;
        this.isTableGroup = isTableGroup;
    }

    public ClientStreamScanner(ObTableClientQueryAsyncStreamResult streamResult, String tableName,
                               byte[] family, boolean isTableGroup) {
        this.streamResult = streamResult;
        this.tableName = tableName;
        this.family = family;
        this.isTableGroup = isTableGroup;
    }

    @Override
    public Result next() throws IOException {
        try {
            checkStatus();
            List<ObObj> startRow;
            if (streamResult.next()) {
                startRow = streamResult.getRow();
            } else {
                return null;
            }

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
            List<Cell> keyValues = new ArrayList<Cell>();
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
            return Result.create(keyValues);
        } catch (Exception e) {
            logger.error(LCD.convert("01-00000"), streamResult.getTableName(), e);
            throw new IOException(String.format("get table %s stream next result error ",
                streamResult.getTableName()), e);
        }
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
        try {
            streamResult.renewLease();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    void checkStatus() throws IllegalStateException {
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
