package com.alipay.oceanbase.hbase.result;

import com.alipay.oceanbase.hbase.util.TableHBaseLoggerFactory;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.stream.async.ObTableClientQueryAsyncStreamResult;
import com.alipay.oceanbase.rpc.stream.async.ObTableQueryAsyncResultSet;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.hbase.util.TableHBaseLoggerFactory.LCD;

public class ClientAsyncStreamScanner extends AbstractClientScanner {

    private static final Logger logger     = TableHBaseLoggerFactory
            .getLogger(ClientAsyncStreamScanner.class);


    //private final ObTableClientQueryAsyncStreamResult asyncStreamResult;

    // private final ObTableQueryAsyncResultSet queryResultSet;
    private final QueryResultSet queryResultSet;
    //private final QueryStreamResult queryStreamResult;

    private final String                         tableName;

    private final byte[]                         family;

    private boolean                              closed     = false;

    private boolean                              streamNext = true;

    private Map<String, Object>                  lastRow = null;

//    public ClientAsyncStreamScanner(ObTableClientQueryAsyncStreamResult asyncStreamResult, String tableName,
//                               byte[] family) {
//        this.asyncStreamResult = asyncStreamResult;
//        this.tableName = tableName;
//        this.family = family;
//    }

    public ClientAsyncStreamScanner(QueryResultSet queryResultSet,
                                    String tableName,
                                    byte[] family) {
        this.queryResultSet = queryResultSet;
        this.tableName = tableName;
        this.family = family;
    }

    @Override
    public Result next() throws IOException {
        try {
            checkStatus();

            if (!streamNext) {
                return null;
            }

            List<ObObj> startRow;
            Map<String, Object> value;

            if (this.lastRow == null) {         // last row get from cacheRows
                if (queryResultSet.next()) {
                    value = queryResultSet.getRow();
                } else {
                    return null;
                }
            } else {
                value = this.lastRow;
            }

            if (value.isEmpty()) {
                return null;
            }

            byte[] sk = (byte[]) value.get("K");
            byte[] sq = (byte[]) value.get("Q");
            long st = (Long) value.get("T");
            byte[] sv = (byte[]) value.get("V");

            KeyValue startKeyValue = new KeyValue(sk, family, sq, st, sv);
            List<KeyValue> keyValues = new ArrayList<KeyValue>();
            keyValues.add(startKeyValue);

            while (streamNext = queryResultSet.next()) {
                Map<String, Object> row = queryResultSet.getRow();
                byte[] k = (byte[]) row.get("K");
                byte[] q = (byte[]) row.get("Q");
                long t = (Long) row.get("T");
                byte[] v = (byte[]) row.get("V");
                if (Arrays.equals(sk, k)) {// when rowKey is equal to the previous rowKey ,merge the result into the same result
                    keyValues.add(new KeyValue(k, family, q, t, v));
                } else {
                    this.lastRow = row;
                    break;
                }
            }
            return new Result(keyValues);
        } catch (Exception e) {
            logger.error(LCD.convert("01-00000"), tableName, e);
            throw new IOException(String.format("get table %s stream next result error ",
                    tableName), e);
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
            queryResultSet.close();
        } catch (Exception e) {
            logger.error(LCD.convert("01-00001"), tableName, e);
        }
    }
}
