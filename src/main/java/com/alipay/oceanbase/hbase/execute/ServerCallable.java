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

package com.alipay.oceanbase.hbase.execute;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.OperationExecuteAble;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public abstract class ServerCallable<T> implements Callable<T> {

    protected final Configuration conf;
    protected final ObTableClient obTableClient;
    protected final OperationExecuteAble executeAble;
    protected final String        tableNameString;
    protected int                 callTimeout;
    protected long                globalStartTime, endTime;
    protected byte[]              startRow, endRow;

    /**
     * ServerCallable
     * @param conf the conf to use
     * @param obTableClient obTableClient to use
     * @param tableNameString Table name to which <code>tableNameString</code> belongs.
     * @param startRow start row
     * @param endRow end row
     * @param callTimeout timeout
     */
    public ServerCallable(Configuration conf, ObTableClient obTableClient, String tableNameString,
                          byte[] startRow, byte[] endRow, int callTimeout) {
        this.conf = conf;
        this.obTableClient = obTableClient;
        this.executeAble = null;
        this.tableNameString = tableNameString;
        this.callTimeout = callTimeout;
        this.startRow = startRow;
        this.endRow = endRow;
    }

    /**
     * ServerCallable
     * @param conf the conf to use
     * @param executeAble executeAble to use
     * @param tableNameString Table name to which <code>tableNameString</code> belongs.
     * @param startRow start row
     * @param endRow end row
     * @param callTimeout timeout
     */
    public ServerCallable(Configuration conf, OperationExecuteAble executeAble, String tableNameString,
                          byte[] startRow, byte[] endRow, int callTimeout) {
        this.conf = conf;
        this.obTableClient = null;
        this.executeAble = executeAble;
        this.tableNameString = tableNameString;
        this.callTimeout = callTimeout;
        this.startRow = startRow;
        this.endRow = endRow;
    }

    public void afterCall() {
        this.endTime = System.currentTimeMillis();
    }

    public void testConnectWhileIdle() {

    }

    public void shouldRetry(Throwable throwable) throws IOException {
        if (throwable instanceof IOException) {
            // Do not retry when connection is interrupted
            throw (IOException) throwable;
        }
        if (this.callTimeout != HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT)
            if ((this.endTime - this.globalStartTime > this.callTimeout)) {
                throw (SocketTimeoutException) new SocketTimeoutException(
                    "Call to access row '" + Bytes.toString(startRow) + "' to '"
                            + Bytes.toString(endRow) + "' on table '" + tableNameString
                            + "' failed on socket timeout exception: " + throwable)
                    .initCause(throwable);
            }
    }

    /**
     * @return {@link com.alipay.oceanbase.rpc.table.ObTable}instance used by this Callable.
     */
    ObTableClient getObTableClient() {
        return this.obTableClient;
    }

    /**
     * Run this instance with retries, timed waits,
     * and refinds of missing regions.
     *
     * @return an object of type T
     * @throws IOException      if a remote or network exception occurs
     * @throws RuntimeException other unspecified error
     */
    public T withRetries() throws IOException, RuntimeException {
        final long pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
            HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
        final int numRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
            HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
        globalStartTime = System.currentTimeMillis();
        List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions = new ArrayList<RetriesExhaustedException.ThrowableWithExtraContext>();
        for (int tries = 0; tries < numRetries; tries++) {
            try {
                testConnectWhileIdle();
                T result = call();
                afterCall();
                return result;
            } catch (Throwable t) {
                afterCall();
                shouldRetry(t);
                RetriesExhaustedException.ThrowableWithExtraContext qt = new RetriesExhaustedException.ThrowableWithExtraContext(
                    t, System.currentTimeMillis(), toString());
                exceptions.add(qt);
                if (tries == numRetries - 1) {
                    StringBuilder buffer = new StringBuilder("Failed contacting ");
                    buffer.append(exceptions);
                    buffer.append(" after ");
                    buffer.append(tries + 1);
                    buffer.append(" attempts.\nExceptions:\n");
                    for (RetriesExhaustedException.ThrowableWithExtraContext e : exceptions) {
                        buffer.append(e.toString());
                        buffer.append("\n");
                    }
                    throw new RetriesExhaustedException(buffer.toString());
                }
            }
            try {
                Thread.sleep(ConnectionUtils.getPauseTime(pause, tries));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Giving up after tries=" + tries, e);
            }
        }
        return null;
    }

    public T withoutRetries() throws IOException, RuntimeException {
        return withoutRetries(false);
    }

    /**
     * Run this instance against the server once.
     *
     * @param testWhileIdle test while idle
     * @return an object of type T
     * @throws IOException      if a remote or network exception occurs
     * @throws RuntimeException other unspecified error
     */
    public T withoutRetries(boolean testWhileIdle) throws IOException, RuntimeException {
        try {
            if (testWhileIdle) {
                testConnectWhileIdle();
            }
            return call();
        } catch (Throwable t) {
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new RuntimeException(t);
            }
        } finally {
            afterCall();
        }
    }
}
