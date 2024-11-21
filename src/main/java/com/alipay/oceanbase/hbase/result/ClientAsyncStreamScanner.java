package com.alipay.oceanbase.hbase.result;

import com.alipay.oceanbase.hbase.util.OHBaseFuncUtils;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryAsyncStreamResult;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryStreamResult;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.concurrent.LinkedBlockingQueue;

import static com.alipay.oceanbase.hbase.util.TableHBaseLoggerFactory.LCD;

public class ClientAsyncStreamScanner extends ClientStreamScanner {
    private Queue<Result> cache;
    private long maxCacheSize;
    private AtomicLong cacheSizeInBytes;
    long maxResultSize;
    // exception queue (from prefetch to main scan execution)
    private Queue<Exception> exceptionsQueue;
    // prefetch thread to be executed asynchronously
    private Thread prefetcher;
    // used for testing
    private Consumer<Boolean> prefetchListener = null;
    private boolean streamNext = true;

    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    public ClientAsyncStreamScanner(ObTableClientQueryAsyncStreamResult streamResult, String tableName, byte[] family, boolean isTableGroup, long maxResultSize) throws Exception {
        super(streamResult, tableName, family, isTableGroup);
        this.maxResultSize = maxResultSize;
        initCache();
    }

    public ClientAsyncStreamScanner(ObTableClientQueryStreamResult streamResult, String tableName, byte[] family, boolean isTableGroup, long maxResultSize) throws Exception {
        super(streamResult, tableName, family, isTableGroup);
        this.maxResultSize = maxResultSize;
        initCache();
    }

    @VisibleForTesting
    public void setPrefetchListener(Consumer<Boolean> prefetchListener) {
        this.prefetchListener = prefetchListener;
    }

    private void initCache() {
        // concurrent cache
        maxCacheSize = maxResultSize > Long.MAX_VALUE / 2 ? maxResultSize : maxResultSize * 2;
        cache = new LinkedBlockingQueue<>();
        cacheSizeInBytes = new AtomicLong(0);
        exceptionsQueue = new ConcurrentLinkedQueue<>();
        prefetcher = new Thread(new PrefetchRunnable());
        Threads.setDaemonThreadRunning(prefetcher, tableName + ".asyncPrefetcher");
    }

    private void loadCache() throws Exception {
        if (streamResult.getRowIndex() == -1 && !streamResult.next()) {
            streamNext = false;
            return;
        }

        long addSize = 0;
        while (!streamResult.getCacheRows().isEmpty()) {
            try {
                checkStatus();

                List<ObObj> startRow = streamResult.getRow();

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
                List<Cell> keyValues = new ArrayList<>();
                keyValues.add(startKeyValue);
                addSize = 0;
                while ((streamNext = streamResult.next())) {
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
                        KeyValue kv = new KeyValue(k, family, q, t, v);
                        addSize += PrivateCellUtil.estimatedSizeOfCell(kv);
                        keyValues.add(kv);
                    } else {
                        break;
                    }
                }
                cache.add(Result.create(keyValues));
                addEstimatedSize(addSize);
            } catch (Exception e) {
                logger.error(LCD.convert("01-00000"), streamResult.getTableName(), e);
                throw new IOException(String.format("get table %s stream next result error ",
                        streamResult.getTableName()), e);
            }
        }
    }

    @Override
    public Result next() throws IOException {
        try {
            lock.lock();
            while (cache.isEmpty() && streamNext) {
                handleException();
                if (this.closed) {
                    return null;
                }
                try {
                    notEmpty.await();
                } catch (InterruptedException e) {
                    throw new InterruptedIOException("Interrupted when wait to load cache");
                }
            }

            Result result = pollCache();
            if (prefetchCondition()) {
                notFull.signalAll();
            }
            return result;
        } finally {
            lock.unlock();
            handleException();
        }
    }

    @Override
    public void close() {
        try {
            lock.lock();
            super.close();
            closed = true;
            notFull.signalAll();
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void addEstimatedSize(long estimatedSize) {
        cacheSizeInBytes.addAndGet(estimatedSize);
    }

    private void handleException() throws IOException {
        //The prefetch task running in the background puts any exception it
        //catches into this exception queue.
        // Rethrow the exception so the application can handle it.
        while (!exceptionsQueue.isEmpty()) {
            Exception first = exceptionsQueue.peek();
            first.printStackTrace();
            if (first instanceof IOException) {
                throw (IOException) first;
            }
            throw (RuntimeException) first;
        }
    }

    private boolean prefetchCondition() {
        return cacheSizeInBytes.get() < maxCacheSize / 2;
    }

    private long estimatedResultSize(Result res) {
        long result_size = 0;
        for (Cell cell : res.rawCells()) {
            result_size += PrivateCellUtil.estimatedSizeOfCell(cell);
        }
        return result_size;
    }

    private Result pollCache() {
        Result res = cache.poll();
        if (null != res) {
            long estimatedSize = estimatedResultSize(res);
            addEstimatedSize(-estimatedSize);
        }
        return res;
    }

    private class PrefetchRunnable implements Runnable {
        @Override
        public void run() {
            while (!closed) {
                boolean succeed = false;
                try {
                    lock.lock();
                    while (!prefetchCondition()) {
                        notFull.await();
                    }
                    loadCache();
                    succeed = true;
                } catch (Exception e) {
                    exceptionsQueue.add(e);
                } finally {
                    notEmpty.signalAll();
                    lock.unlock();
                    if (prefetchListener != null) {
                        prefetchListener.accept(succeed);
                    }
                }
            }
        }
    }
}