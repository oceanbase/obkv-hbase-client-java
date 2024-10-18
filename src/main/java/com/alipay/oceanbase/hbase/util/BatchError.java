package com.alipay.oceanbase.hbase.util;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;

import java.util.ArrayList;
import java.util.List;

public class BatchError {
    private final List<Throwable> throwables = new ArrayList<Throwable>();
    private final List<Row> actions = new ArrayList<Row>();
    private final List<String> addresses = new ArrayList<String>();

    public synchronized void add(Throwable ex, Row row, ServerName serverName) {
        if (row == null){
            throw new IllegalArgumentException("row cannot be null. location=" + serverName);
        }

        throwables.add(ex);
        actions.add(row);
        addresses.add(serverName != null ? serverName.toString() : "null");
    }

    public boolean hasErrors() {
        return !throwables.isEmpty();
    }

    public synchronized RetriesExhaustedWithDetailsException makeException() {
        return new RetriesExhaustedWithDetailsException(
                new ArrayList<Throwable>(throwables),
                new ArrayList<Row>(actions), new ArrayList<String>(addresses));
    }

    public synchronized void clear() {
        throwables.clear();
        actions.clear();
        addresses.clear();
    }
}
