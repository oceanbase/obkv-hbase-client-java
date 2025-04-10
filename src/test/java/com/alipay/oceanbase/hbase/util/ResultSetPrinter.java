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

package com.alipay.oceanbase.hbase.util;

import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

public class ResultSetPrinter {
    private static final int              MAX_COL_WIDTH = 30;
    private static final String           VERTICAL      = "│";
    private static final String           HORIZONTAL    = "─";
    private static final String           TOP_LEFT      = "┌";
    private static final String           TOP_RIGHT     = "┐";
    private static final String           MID_LEFT      = "├";
    private static final String           MID_RIGHT     = "┤";
    private static final String           BOTTOM_LEFT   = "└";
    private static final String           BOTTOM_RIGHT  = "┘";
    private static final String           CROSS         = "┼";
    private static final SimpleDateFormat DATE_FORMAT   = new SimpleDateFormat(
                                                            "yyyy-MM-dd HH:mm:ss");

    public static void print(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();

        List<ColumnMeta> columns = new ArrayList<ColumnMeta>();
        for (int i = 1; i <= colCount; i++) {
            columns.add(new ColumnMeta(meta.getColumnLabel(i), meta.getColumnType(i), meta
                .getPrecision(i)));
        }

        // 计算初始列宽
        List<Integer> widths = new ArrayList<Integer>();
        for (ColumnMeta col : columns) {
            int width = Math.min(Math.max(col.name.length(), getTypeWidth(col)), MAX_COL_WIDTH);
            widths.add(width);
        }

        List<List<String>> rows = new ArrayList<List<String>>();
        while (rs.next()) {
            List<String> row = new ArrayList<String>();
            for (int i = 0; i < colCount; i++) {
                Object value = rs.getObject(i + 1);
                String str = formatValue(value, widths.get(i));

                int actualWidth = str.length();
                if (actualWidth > widths.get(i)) {
                    widths.set(i, Math.min(actualWidth, MAX_COL_WIDTH));
                }
                row.add(str);
            }
            rows.add(row);
        }

        StringBuilder fmt = new StringBuilder(VERTICAL);
        for (int w : widths) {
            fmt.append(" %-").append(w).append("s ").append(VERTICAL);
        }

        printDivider(widths, TOP_LEFT, CROSS, TOP_RIGHT);
        System.out.printf(fmt.toString() + "%n", getHeaders(columns));
        printDivider(widths, MID_LEFT, CROSS, MID_RIGHT);
        for (List<String> row : rows) {
            System.out.printf(fmt.toString() + "%n", row.toArray());
        }
        printDivider(widths, BOTTOM_LEFT, CROSS, BOTTOM_RIGHT);
    }

    private static class ColumnMeta {
        final String name;
        final int    type;
        final int    precision;

        ColumnMeta(String name, int type, int precision) {
            this.name = name;
            this.type = type;
            this.precision = precision;
        }
    }

    private static int getTypeWidth(ColumnMeta col) {
        switch (col.type) {
            case Types.DATE:
                return 10;
            case Types.TIMESTAMP:
                return 19;
            case Types.INTEGER:
                return 11;
            case Types.DECIMAL:
            case Types.NUMERIC:
                return col.precision + 2;
            default:
                return Math.min(col.precision, MAX_COL_WIDTH);
        }
    }

    private static String formatValue(Object value, int maxWidth) {
        if (value == null)
            return "NULL";

        if (value instanceof byte[]) {
            return formatByteArray((byte[]) value, maxWidth);
        }
        if (value instanceof Date) {
            return DATE_FORMAT.format((Date) value);
        }
        return truncateString(value.toString(), maxWidth);
    }

    private static String formatByteArray(byte[] bytes, int maxWidth) {
        if (bytes.length == 0)
            return "[NULL]";
        if (isPrintable(bytes)) {
            return truncateString(new String(bytes), maxWidth);
        }

        if (bytes.length <= 4) {
            return hexFormat(bytes, maxWidth);
        }

        return base64Format(bytes, maxWidth);
    }

    private static boolean isPrintable(byte[] bytes) {
        for (byte b : bytes) {
            if (b < 0x20 || b > 0x7E)
                return false;
        }
        return true;
    }

    private static String hexFormat(byte[] bytes, int maxWidth) {
        int maxBytes = (maxWidth - 3) / 3; // [XX XX...]
        maxBytes = Math.max(1, maxBytes);

        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < Math.min(bytes.length, maxBytes); i++) {
            sb.append(String.format("%02X ", bytes[i]));
        }
        if (bytes.length > maxBytes)
            sb.append("...");
        sb.setLength(sb.length() - 1);
        return sb.append("]").toString();
    }

    private static String base64Format(byte[] bytes, int maxWidth) {
        String base64 = Base64.getEncoder().encodeToString(bytes);
        return truncateString("b64:" + base64, maxWidth);
    }

    private static String truncateString(String str, int maxWidth) {
        if (str.length() <= maxWidth)
            return str;
        return str.substring(0, maxWidth - 3) + "...";
    }

    private static Object[] getHeaders(List<ColumnMeta> columns) {
        String[] headers = new String[columns.size()];
        for (int i = 0; i < headers.length; i++) {
            headers[i] = columns.get(i).name;
        }
        return headers;
    }

    private static void printDivider(List<Integer> widths, String left, String mid, String right) {
        StringBuilder sb = new StringBuilder(left);
        for (int i = 0; i < widths.size(); i++) {
            sb.append(String.join("", Collections.nCopies(widths.get(i) + 2, HORIZONTAL)));
            sb.append(i < widths.size() - 1 ? mid : right);
        }
        System.out.println(sb);
    }

}
