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

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.hbase.util.TableTemplateManager.TableType.*;
import static com.alipay.oceanbase.hbase.util.TableTemplateManager.TableType.SECONDARY_PARTITIONED_KEY_RANGE_GEN;

public class TableTemplateManager {
    public static final long   PART_NUM           = 3;
    public static final String TABLE_GROUP_PREFIX = "test_group_";
    public static final String COLUMN_FAMILY      = "cf";

    public enum TableType {
        NON_PARTITIONED_REGULAR, NON_PARTITIONED_TIME_SERIES, SINGLE_PARTITIONED_REGULAR, SINGLE_PARTITIONED_TIME_SERIES, SECONDARY_PARTITIONED_RANGE_KEY, // RANGE-KEY分区（使用K）
        SECONDARY_PARTITIONED_RANGE_KEY_GEN, // RANGE-KEY分区（使用生成列）
        SECONDARY_PARTITIONED_KEY_RANGE, // KEY-RANGE分区（使用K）
        SECONDARY_PARTITIONED_KEY_RANGE_GEN, // KEY-RANGE分区（使用生成列）
        SECONDARY_PARTITIONED_TIME_RANGE_KEY, // 时序表RANGE-KEY
        SECONDARY_PARTITIONED_TIME_KEY_RANGE, // 时序表KEY-RANGE

        /* ------------------ CELL TTL ----------------*/
        NON_PARTITIONED_REGULAR_CELL_TTL, SINGLE_PARTITIONED_REGULAR_CELL_TTL, SECONDARY_PARTITIONED_RANGE_KEY_CELL_TTL, // RANGE-KEY分区（使用K）
        SECONDARY_PARTITIONED_RANGE_KEY_GEN_CELL_TTL, // RANGE-KEY分区（使用生成列）
        SECONDARY_PARTITIONED_KEY_RANGE_CELL_TTL, // KEY-RANGE分区（使用K）
        SECONDARY_PARTITIONED_KEY_RANGE_GEN_CELL_TTL, // KEY-RANGE分区（使用生成列）
    }

    public static List<TableType>               NORMAL_AND_SERIES_TABLES = Arrays
                                                                             .asList(
                                                                                 NON_PARTITIONED_REGULAR,
                                                                                 NON_PARTITIONED_TIME_SERIES,
                                                                                 SINGLE_PARTITIONED_REGULAR,
                                                                                 SINGLE_PARTITIONED_TIME_SERIES,
                                                                                 SECONDARY_PARTITIONED_RANGE_KEY,
                                                                                 SECONDARY_PARTITIONED_RANGE_KEY_GEN,
                                                                                 SECONDARY_PARTITIONED_KEY_RANGE,
                                                                                 SECONDARY_PARTITIONED_KEY_RANGE_GEN,
                                                                                 SECONDARY_PARTITIONED_TIME_RANGE_KEY,
                                                                                 SECONDARY_PARTITIONED_TIME_KEY_RANGE);

    public static List<TableType>               NORMAL_TABLES            = Arrays
                                                                             .asList(
                                                                                 NON_PARTITIONED_REGULAR,
                                                                                 SINGLE_PARTITIONED_REGULAR,
                                                                                 SECONDARY_PARTITIONED_RANGE_KEY,
                                                                                 SECONDARY_PARTITIONED_RANGE_KEY_GEN,
                                                                                 SECONDARY_PARTITIONED_KEY_RANGE,
                                                                                 SECONDARY_PARTITIONED_KEY_RANGE_GEN);

    public static List<TableType>               CELL_TTL_TABLES          = Arrays
                                                                             .asList(
                                                                                 NON_PARTITIONED_REGULAR_CELL_TTL,
                                                                                 SINGLE_PARTITIONED_REGULAR_CELL_TTL,
                                                                                 SECONDARY_PARTITIONED_RANGE_KEY_CELL_TTL,
                                                                                 SECONDARY_PARTITIONED_RANGE_KEY_GEN_CELL_TTL,
                                                                                 SECONDARY_PARTITIONED_KEY_RANGE_CELL_TTL,
                                                                                 SECONDARY_PARTITIONED_KEY_RANGE_GEN_CELL_TTL);

    public static List<TableType> TIMESERIES_TABLES = Arrays.asList(NON_PARTITIONED_TIME_SERIES,
            SINGLE_PARTITIONED_TIME_SERIES,
            SECONDARY_PARTITIONED_TIME_RANGE_KEY,
            SECONDARY_PARTITIONED_TIME_KEY_RANGE);

    private static final Map<TableType, String> SQL_TEMPLATES            = new EnumMap<TableType, String>(
                                                                             TableType.class);

    private static final Map<TableType, String> SQL_TEMPLATES = new EnumMap<>(TableType.class);

    static {
        // 普通表非分区表模版
        SQL_TEMPLATES.put(TableType.NON_PARTITIONED_REGULAR,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `Q` varbinary(256) NOT NULL,\n" + "  `T` bigint(20) NOT NULL,\n"
                    + "  `V` varbinary(1024) DEFAULT NULL,\n" + "  PRIMARY KEY (`K`, `Q`, `T`)\n"
                    + ") TABLEGROUP = %s");
        // 时序表非分区表模版
        SQL_TEMPLATES.put(TableType.NON_PARTITIONED_TIME_SERIES,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `T` bigint(20) NOT NULL,\n" + "  `S` bigint(20) NOT NULL,\n"
                    + "  `V` json NOT NULL,\n" + "  PRIMARY KEY (`K`, `T`, `S`)\n"
                    + ") TABLEGROUP = %s");
        // 普通表一级分区模板
        SQL_TEMPLATES.put(TableType.SINGLE_PARTITIONED_REGULAR,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `Q` varbinary(256) NOT NULL,\n" + "  `T` bigint(20) NOT NULL,\n"
                    + "  `V` varbinary(1024) DEFAULT NULL,\n" + "  PRIMARY KEY (`K`, `Q`, `T`)\n"
                    + ") TABLEGROUP = %s PARTITION BY KEY(`K`) PARTITIONS %d ");
        // 时序表一级分区模板
        SQL_TEMPLATES.put(TableType.SINGLE_PARTITIONED_TIME_SERIES,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `T` bigint(20) NOT NULL,\n" + "  `S` bigint(20) NOT NULL,\n"
                    + "  `V` json NOT NULL,\n" + "  PRIMARY KEY (`K`, `T`, `S`)\n"
                    + ") TABLEGROUP = %s PARTITION BY KEY(`K`) PARTITIONS %d ");
        // 普通表RANGE-KEY分区（使用K）
        SQL_TEMPLATES.put(TableType.SECONDARY_PARTITIONED_RANGE_KEY,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `Q` varbinary(256) NOT NULL,\n" + "  `T` bigint(20) NOT NULL,\n"
                    + "  `V` varbinary(1024) DEFAULT NULL,\n"
                    + "  `G` bigint(20) GENERATED ALWAYS AS (ABS(`T`))%s,\n"
                    + "  PRIMARY KEY (`K`, `Q`, `T`)\n"
                    + ") TABLEGROUP = %s PARTITION BY RANGE COLUMNS(`G`) \n"
                    + "SUBPARTITION BY KEY(`%s`) SUBPARTITIONS %d \n"
                    + "(PARTITION `p0` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p1` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p2` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p3` VALUES LESS THAN MAXVALUE)");

        // 合并GEN类型的注释处理
        SQL_TEMPLATES.put(TableType.SECONDARY_PARTITIONED_RANGE_KEY_GEN,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `Q` varbinary(256) NOT NULL,\n" + "  `T` bigint(20) NOT NULL,\n"
                    + "  `V` varbinary(1024) DEFAULT NULL,\n"
                    + "  `G` bigint(20) GENERATED ALWAYS AS (ABS(`T`))%s,\n"
                    + "  PRIMARY KEY (`K`, `Q`, `T`)\n"
                    + ") TABLEGROUP = %s PARTITION BY RANGE COLUMNS(`G`) \n"
                    + "SUBPARTITION BY KEY(`%s`) SUBPARTITIONS %d \n"
                    + "(PARTITION `p0` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p1` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p2` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p3` VALUES LESS THAN MAXVALUE) ");

        // 普通表KEY-RANGE分区（使用K）
        SQL_TEMPLATES.put(TableType.SECONDARY_PARTITIONED_KEY_RANGE,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `Q` varbinary(256) NOT NULL,\n" + "  `T` bigint(20) NOT NULL,\n"
                    + "  `V` varbinary(1024) DEFAULT NULL,\n"
                    + "  `G` bigint(20) GENERATED ALWAYS AS (ABS(`T`))%s,\n"
                    + "  PRIMARY KEY (`K`, `Q`, `T`)\n"
                    + ") TABLEGROUP = %s PARTITION BY KEY(`%s`) PARTITIONS %d \n"
                    + "SUBPARTITION BY RANGE COLUMNS(`G`) \n" + "SUBPARTITION TEMPLATE (\n"
                    + "  SUBPARTITION `p0` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p1` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p2` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p3` VALUES LESS THAN MAXVALUE) ");

        // 普通表KEY-RANGE分区（使用生成列）
        SQL_TEMPLATES.put(TableType.SECONDARY_PARTITIONED_KEY_RANGE_GEN,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `Q` varbinary(256) NOT NULL,\n" + "  `T` bigint(20) NOT NULL,\n"
                    + "  `V` varbinary(1024) DEFAULT NULL,\n"
                    + "  `G` bigint(20) GENERATED ALWAYS AS (ABS(`T`))%s,\n"
                    + "  PRIMARY KEY (`K`, `Q`, `T`)\n"
                    + ") TABLEGROUP = %s PARTITION BY KEY(`%s`) PARTITIONS %d \n"
                    + "SUBPARTITION BY RANGE COLUMNS(`G`) \n" + "SUBPARTITION TEMPLATE (\n"
                    + "  SUBPARTITION `p0` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p1` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p2` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p3` VALUES LESS THAN MAXVALUE) ");

        // 时序表RANGE-KEY分区
        SQL_TEMPLATES.put(TableType.SECONDARY_PARTITIONED_TIME_RANGE_KEY,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `T` bigint(20) NOT NULL,\n" + "  `S` bigint(20) NOT NULL,\n"
                    + "  `V` json NOT NULL,\n"
                    + "  `G` bigint(20) GENERATED ALWAYS AS (ABS(`T`))%s,\n"
                    + "  PRIMARY KEY (`K`, `T`, `S`)\n"
                    + ") TABLEGROUP = %s PARTITION BY RANGE COLUMNS(`G`) \n"
                    + "SUBPARTITION BY KEY(`%s`) SUBPARTITIONS %d \n"
                    + "(PARTITION `p0` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p1` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p2` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p3` VALUES LESS THAN MAXVALUE) ");

        // 时序表KEY-RANGE分区
        SQL_TEMPLATES.put(TableType.SECONDARY_PARTITIONED_TIME_KEY_RANGE,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `T` bigint(20) NOT NULL,\n" + "  `S` bigint(20) NOT NULL,\n"
                    + "  `V` json NOT NULL,\n"
                    + "  `G` bigint(20) GENERATED ALWAYS AS (ABS(`T`))%s,\n"
                    + "  PRIMARY KEY (`K`, `T`, `S`)\n"
                    + ") TABLEGROUP = %s PARTITION BY KEY(`%s`) PARTITIONS %d \n"
                    + "SUBPARTITION BY RANGE COLUMNS(`G`) \n" + "SUBPARTITION TEMPLATE (\n"
                    + "  SUBPARTITION `p0` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p1` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p2` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p3` VALUES LESS THAN MAXVALUE)");

        /* ------------------ CELL TTL ----------------*/
        SQL_TEMPLATES.put(TableType.NON_PARTITIONED_REGULAR_CELL_TTL,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `Q` varbinary(256) NOT NULL,\n" + "  `T` bigint(20) NOT NULL,\n"
                    + "  `V` varbinary(1024) DEFAULT NULL,\n"
                    + "  `TTL` bigint(20) DEFAULT NULL,\n" + "  PRIMARY KEY (`K`, `Q`, `T`)\n"
                    + ") TABLEGROUP = %s");

        SQL_TEMPLATES.put(TableType.SINGLE_PARTITIONED_REGULAR_CELL_TTL,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `Q` varbinary(256) NOT NULL,\n" + "  `T` bigint(20) NOT NULL,\n"
                    + "  `V` varbinary(1024) DEFAULT NULL,\n"
                    + "  `TTL` bigint(20) DEFAULT NULL,\n" + "  PRIMARY KEY (`K`, `Q`, `T`)\n"
                    + ") TABLEGROUP = %s PARTITION BY KEY(`K`) PARTITIONS %d ");

        SQL_TEMPLATES.put(TableType.SECONDARY_PARTITIONED_RANGE_KEY_CELL_TTL,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `Q` varbinary(256) NOT NULL,\n" + "  `T` bigint(20) NOT NULL,\n"
                    + "  `V` varbinary(1024) DEFAULT NULL,\n"
                    + "  `TTL` bigint(20) DEFAULT NULL,\n"
                    + "  `G` bigint(20) GENERATED ALWAYS AS (ABS(`T`))%s,\n"
                    + "  PRIMARY KEY (`K`, `Q`, `T`)\n"
                    + ") TABLEGROUP = %s PARTITION BY RANGE COLUMNS(`G`) \n"
                    + "SUBPARTITION BY KEY(`%s`) SUBPARTITIONS %d \n"
                    + "(PARTITION `p0` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p1` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p2` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p3` VALUES LESS THAN MAXVALUE)");

        SQL_TEMPLATES.put(TableType.SECONDARY_PARTITIONED_RANGE_KEY_GEN_CELL_TTL,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `Q` varbinary(256) NOT NULL,\n" + "  `T` bigint(20) NOT NULL,\n"
                    + "  `V` varbinary(1024) DEFAULT NULL,\n"
                    + "  `TTL` bigint(20) DEFAULT NULL,\n"
                    + "  `G` bigint(20) GENERATED ALWAYS AS (ABS(`T`))%s,\n"
                    + "  PRIMARY KEY (`K`, `Q`, `T`)\n"
                    + ") TABLEGROUP = %s PARTITION BY RANGE COLUMNS(`G`) \n"
                    + "SUBPARTITION BY KEY(`%s`) SUBPARTITIONS %d \n"
                    + "(PARTITION `p0` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p1` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p2` VALUES LESS THAN (%d),\n"
                    + " PARTITION `p3` VALUES LESS THAN MAXVALUE) ");

        SQL_TEMPLATES.put(TableType.SECONDARY_PARTITIONED_KEY_RANGE_CELL_TTL,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `Q` varbinary(256) NOT NULL,\n" + "  `T` bigint(20) NOT NULL,\n"
                    + "  `V` varbinary(1024) DEFAULT NULL,\n"
                    + "  `TTL` bigint(20) DEFAULT NULL,\n"
                    + "  `G` bigint(20) GENERATED ALWAYS AS (ABS(`T`))%s,\n"
                    + "  PRIMARY KEY (`K`, `Q`, `T`)\n"
                    + ") TABLEGROUP = %s PARTITION BY KEY(`%s`) PARTITIONS %d \n"
                    + "SUBPARTITION BY RANGE COLUMNS(`G`) \n" + "SUBPARTITION TEMPLATE (\n"
                    + "  SUBPARTITION `p0` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p1` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p2` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p3` VALUES LESS THAN MAXVALUE) ");

        SQL_TEMPLATES.put(TableType.SECONDARY_PARTITIONED_KEY_RANGE_GEN_CELL_TTL,
            "CREATE TABLE IF NOT EXISTS `%s` (\n" + "  `K` varbinary(1024) NOT NULL,\n"
                    + "  `Q` varbinary(256) NOT NULL,\n" + "  `T` bigint(20) NOT NULL,\n"
                    + "  `V` varbinary(1024) DEFAULT NULL,\n"
                    + "  `TTL` bigint(20) DEFAULT NULL,\n"
                    + "  `G` bigint(20) GENERATED ALWAYS AS (ABS(`T`))%s,\n"
                    + "  PRIMARY KEY (`K`, `Q`, `T`)\n"
                    + ") TABLEGROUP = %s PARTITION BY KEY(`%s`) PARTITIONS %d \n"
                    + "SUBPARTITION BY RANGE COLUMNS(`G`) \n" + "SUBPARTITION TEMPLATE (\n"
                    + "  SUBPARTITION `p0` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p1` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p2` VALUES LESS THAN (%d),\n"
                    + "  SUBPARTITION `p3` VALUES LESS THAN MAXVALUE) ");
    }

    public static String getCreateTableSQL(TableType type, String tableName,
                                           TimeGenerator.TimeRange timeRange) {
        System.out.println(tableName);
        String template = SQL_TEMPLATES.get(type);
        Object[] params;
        String tableGroup = extractTableGroup(tableName);

        switch (type) {
            case NON_PARTITIONED_REGULAR:
            case NON_PARTITIONED_TIME_SERIES:
            case NON_PARTITIONED_REGULAR_CELL_TTL:
                params = new Object[] { tableName, tableGroup };
                break;
            case SINGLE_PARTITIONED_REGULAR:
            case SINGLE_PARTITIONED_TIME_SERIES: // 合并相同处理逻辑
            case SINGLE_PARTITIONED_REGULAR_CELL_TTL:
                params = new Object[] { tableName, tableGroup, PART_NUM };
                break;
            case SECONDARY_PARTITIONED_RANGE_KEY:
            case SECONDARY_PARTITIONED_RANGE_KEY_GEN:
            case SECONDARY_PARTITIONED_KEY_RANGE:
            case SECONDARY_PARTITIONED_KEY_RANGE_GEN:
            case SECONDARY_PARTITIONED_RANGE_KEY_CELL_TTL:
            case SECONDARY_PARTITIONED_RANGE_KEY_GEN_CELL_TTL:
            case SECONDARY_PARTITIONED_KEY_RANGE_CELL_TTL:
            case SECONDARY_PARTITIONED_KEY_RANGE_GEN_CELL_TTL:
                boolean isGen = type.name().contains("GEN");
                params = new Object[] { tableName, getGeneratedColumn(type), tableGroup,
                        isGen ? "K_PREFIX" : "K", PART_NUM, timeRange.lowerBound1(),
                        timeRange.lowerBound1() + 86400000, timeRange.lowerBound1() + 172800000 };
                break;
            case SECONDARY_PARTITIONED_TIME_RANGE_KEY:
            case SECONDARY_PARTITIONED_TIME_KEY_RANGE: // 合并时序表处理
                params = new Object[] { tableName, "", tableGroup, "K", PART_NUM,
                        timeRange.lowerBound1(), timeRange.lowerBound1() + 86400000,
                        timeRange.lowerBound1() + 172800000 };
                break;
            default:
                throw new IllegalArgumentException("Unsupported table type");
        }

        return String.format(template, params);
    }

    private static String getGeneratedColumn(TableType type) {
        StringBuilder sb = new StringBuilder();
        boolean needsKPrefix = type.name().startsWith("SECONDARY_PARTITIONED")
                               && !type.name().contains("TIME") && type.name().contains("GEN");

        if (needsKPrefix) {
            sb.append(",\n  K_PREFIX varbinary(1024) GENERATED ALWAYS AS (substring(`K`, 1, 4))");
        }
        return sb.toString();
    }

    private static String getPartitionStrategy(TableType type) {
        if (type.name().contains("RANGE_KEY")) {
            return type.name().contains("GEN") ? "RANGE COLUMNS(`G`) SUBPARTITION BY KEY(`K_PREFIX`) SUBPARTITIONS "
                                                 + PART_NUM
                : "RANGE COLUMNS(`G`) SUBPARTITION BY KEY(`K`) SUBPARTITIONS " + PART_NUM;
        }
        if (type.name().contains("KEY_RANGE")) {
            return type.name().contains("GEN") ? "KEY(`K_PREFIX`) PARTITIONS " + PART_NUM
                                                 + " SUBPARTITION BY RANGE COLUMNS(`G`)"
                : "KEY(`K`) PARTITIONS " + PART_NUM + " SUBPARTITION BY RANGE COLUMNS(`G`)";
        }
        return "";
    }

    public static String generateTableGroupSQL(String tableGroup) {
        return String
            .format("CREATE TABLEGROUP IF NOT EXISTS %s SHARDING = 'ADAPTIVE'", tableGroup);
    }

    public static String getTableGroupName(TableTemplateManager.TableType type, boolean multiCf) {
        return TABLE_GROUP_PREFIX + type.name().toLowerCase() + (multiCf ? "_mcf" : "");
    }

    public static String generateTableName(String tableGroup, boolean multiCf, int cfIndex) {
        return String
            .format("%s$%s", tableGroup, multiCf ? COLUMN_FAMILY + cfIndex : COLUMN_FAMILY);
    }

    public static String extractTableGroup(String tableName) {
        int dollarIndex = tableName.indexOf('$');
        if (dollarIndex > 0) {
            return tableName.substring(0, dollarIndex);
        }
        throw new IllegalArgumentException("Invalid table name: " + tableName);
    }
}
