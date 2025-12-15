CREATE TABLE `test$testload` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLE `test_t$family1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLE `test$family1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLE `test$cellTTLFamily` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    `TTL` bigint(20) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLE `test_t$partitionFamily1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) partition by key(`K`) partitions 17;

CREATE TABLE `test$familyPartition` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) partition by key(`K`) partitions 17;

CREATE TABLE `test_t$familyPartition` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) partition by key(`K`) partitions 17;

CREATE TABLE `test$partitionFamily1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) partition by key(`K`) partitions 17;

CREATE TABLE `test$familyRange` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) partition by range columns (`K`) (
    PARTITION p0 VALUES LESS THAN ('d'),
    PARTITION p1 VALUES LESS THAN ('j'),
    PARTITION p2 VALUES LESS THAN MAXVALUE
);

CREATE TABLEGROUP test SHARDING = 'ADAPTIVE';
CREATE TABLE `test$family_group` (
      `K` varbinary(1024) NOT NULL,
      `Q` varbinary(256) NOT NULL,
      `T` bigint(20) NOT NULL,
      `V` varbinary(1024) DEFAULT NULL,
      PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test;

CREATE TABLEGROUP test_t SHARDING = 'ADAPTIVE';
CREATE TABLE `test_t$family_group` (
      `K` varbinary(1024) NOT NULL,
      `Q` varbinary(256) NOT NULL,
      `T` bigint(20) NOT NULL,
      `V` varbinary(1024) DEFAULT NULL,
      PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_t;

CREATE TABLE `test_t$family_ttl` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLE `test$family_ttl` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLE `test$familyThrottle` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLEGROUP testAdminKey SHARDING = 'ADAPTIVE';
CREATE TABLE `testAdminKey$familyRange` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = testAdminKey
partition by key(`K`) partitions 17;

CREATE TABLEGROUP testAdminKey_t SHARDING = 'ADAPTIVE';
CREATE TABLE `testAdminKey_t$familyRange` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = testAdminKey_t
partition by key(`K`) partitions 17;

CREATE TABLEGROUP testAdminRange SHARDING = 'ADAPTIVE';
CREATE TABLE `testAdminRange$familyRange` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
)  TABLEGROUP = testAdminRange
partition by range columns (`K`) (
    PARTITION p0 VALUES LESS THAN ('a'),
    PARTITION p1 VALUES LESS THAN ('w'),
    PARTITION p2 VALUES LESS THAN MAXVALUE
);

CREATE TABLEGROUP testAdminRange_t SHARDING = 'ADAPTIVE';
CREATE TABLE `testAdminRange_t$familyRange` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
)  TABLEGROUP = testAdminRange_t
partition by range columns (`K`) (
    PARTITION p0 VALUES LESS THAN ('a'),
    PARTITION p1 VALUES LESS THAN ('w'),
    PARTITION p2 VALUES LESS THAN MAXVALUE
);

CREATE TABLE `test$family_with_local_index` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    key `idx1`(T) local,
    PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLE `test_t$family_with_local_index` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    key `idx1`(T) local,
    PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLE `test$family'1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test;

CREATE TABLE `test_t$family'1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_t;

CREATE TABLEGROUP test_multi_cf SHARDING = 'ADAPTIVE';

CREATE TABLE `test_multi_cf$family_with_group1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`) 
) TABLEGROUP = test_multi_cf PARTITION BY KEY(`K`) PARTITIONS 3;

CREATE TABLE `test_multi_cf$family_with_group2` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`) 
) TABLEGROUP = test_multi_cf PARTITION BY KEY(`K`) PARTITIONS 3;

CREATE TABLE `test_multi_cf$family_with_group3` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`) 
) TABLEGROUP = test_multi_cf PARTITION BY KEY(`K`) PARTITIONS 3;

CREATE DATABASE IF NOT EXISTS `n1`;
USE `n1`;
CREATE TABLE `n1:test$family1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLE `n1:test_t$family1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLEGROUP `n1:test` SHARDING = 'ADAPTIVE';
CREATE TABLE `n1:test$family'1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = `n1:test`;

CREATE TABLE `n1:test$family_with_local_index` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    key `idx1`(T) local,
    PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLE `n1:test$family_group` (
      `K` varbinary(1024) NOT NULL,
      `Q` varbinary(256) NOT NULL,
      `T` bigint(20) NOT NULL,
      `V` varbinary(1024) DEFAULT NULL,
      PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = `n1:test`;

CREATE TABLE `n1:test$partitionFamily1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) partition by key(`K`) partitions 17;

CREATE TABLEGROUP test_region_locator SHARDING = 'ADAPTIVE';
CREATE TABLE `test_region_locator$family_region_locator` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_region_locator PARTITION BY RANGE COLUMNS(K) (
    PARTITION p1 VALUES LESS THAN ('c'),
    PARTITION p2 VALUES LESS THAN ('e'),
    PARTITION p3 VALUES LESS THAN ('g'),
    PARTITION p4 VALUES LESS THAN ('i'),
    PARTITION p5 VALUES LESS THAN ('l'),
    PARTITION p6 VALUES LESS THAN ('n'),
    PARTITION p7 VALUES LESS THAN ('p'),
    PARTITION p8 VALUES LESS THAN ('s'),
    PARTITION p9 VALUES LESS THAN ('v'),
    PARTITION p10 VALUES LESS THAN (MAXVALUE)
);

CREATE TABLEGROUP test_desc SHARDING = 'ADAPTIVE';
CREATE TABLE `test_desc$family1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_desc
KV_ATTRIBUTES ='{"Hbase": {"TimeToLive": 3600, "MaxVersions": 3}}' 
PARTITION BY RANGE COLUMNS(K) (
    PARTITION p1 VALUES LESS THAN ('c'),
    PARTITION p2 VALUES LESS THAN ('e'),
    PARTITION p3 VALUES LESS THAN ('g'),
    PARTITION p4 VALUES LESS THAN ('i'),
    PARTITION p5 VALUES LESS THAN ('l'),
    PARTITION p6 VALUES LESS THAN ('n'),
    PARTITION p7 VALUES LESS THAN ('p'),
    PARTITION p8 VALUES LESS THAN ('s'),
    PARTITION p9 VALUES LESS THAN ('v'),
    PARTITION p10 VALUES LESS THAN (MAXVALUE)
);

CREATE TABLE `test_desc$family2` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_desc
KV_ATTRIBUTES ='{"Hbase": {"TimeToLive": 7200, "MaxVersions": 3}}'
PARTITION BY RANGE COLUMNS(K) (
    PARTITION p1 VALUES LESS THAN ('c'),
    PARTITION p2 VALUES LESS THAN ('e'),
    PARTITION p3 VALUES LESS THAN ('g'),
    PARTITION p4 VALUES LESS THAN ('i'),
    PARTITION p5 VALUES LESS THAN ('l'),
    PARTITION p6 VALUES LESS THAN ('n'),
    PARTITION p7 VALUES LESS THAN ('p'),
    PARTITION p8 VALUES LESS THAN ('s'),
    PARTITION p9 VALUES LESS THAN ('v'),
    PARTITION p10 VALUES LESS THAN (MAXVALUE)
);

CREATE TABLEGROUP test_secondary_key_range SHARDING = 'ADAPTIVE';
CREATE TABLE IF NOT EXISTS `test_secondary_key_range$family1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    `G` bigint(20) GENERATED ALWAYS AS (ABS(T)),
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_secondary_key_range PARTITION BY KEY(`K`) PARTITIONS 3
SUBPARTITION BY RANGE COLUMNS(`G`) SUBPARTITION TEMPLATE (
    SUBPARTITION `p1` VALUES LESS THAN (100),
    SUBPARTITION `p2` VALUES LESS THAN (200),
    SUBPARTITION `p3` VALUES LESS THAN MAXVALUE
);
CREATE TABLE IF NOT EXISTS `test_secondary_key_range$family2` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    `G` bigint(20) GENERATED ALWAYS AS (ABS(T)),
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_secondary_key_range PARTITION BY KEY(`K`) PARTITIONS 3
SUBPARTITION BY RANGE COLUMNS(`G`) SUBPARTITION TEMPLATE (
    SUBPARTITION `p1` VALUES LESS THAN (100),
    SUBPARTITION `p2` VALUES LESS THAN (200),
    SUBPARTITION `p3` VALUES LESS THAN MAXVALUE
);

CREATE TABLEGROUP test_secondary_range_key SHARDING = 'ADAPTIVE';
CREATE TABLE IF NOT EXISTS `test_secondary_range_key$family1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    `G` bigint(20) GENERATED ALWAYS AS (ABS(T)),
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_secondary_range_key PARTITION BY RANGE COLUMNS(`G`)
SUBPARTITION BY KEY(`K`) SUBPARTITIONS 3
(    PARTITION `p1` VALUES LESS THAN (100),
    PARTITION `p2` VALUES LESS THAN (200),
    PARTITION `p3` VALUES LESS THAN MAXVALUE
);

use test;

CREATE TABLEGROUP test_get_optimize_secondary_key_range SHARDING = 'ADAPTIVE';
CREATE TABLE IF NOT EXISTS `test_get_optimize_secondary_key_range$family1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    `G` bigint(20) GENERATED ALWAYS AS (ABS(T)),
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_get_optimize_secondary_key_range
PARTITION BY KEY(`K`) PARTITIONS 3
SUBPARTITION BY RANGE COLUMNS(`G`) SUBPARTITION TEMPLATE (
    SUBPARTITION `p1` VALUES LESS THAN (100),
    SUBPARTITION `p2` VALUES LESS THAN (200),
    SUBPARTITION `p3` VALUES LESS THAN MAXVALUE
);

CREATE TABLEGROUP test_get_optimize_secondary_range_key SHARDING = 'ADAPTIVE';
CREATE TABLE IF NOT EXISTS `test_get_optimize_secondary_range_key$family1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    `G` bigint(20) GENERATED ALWAYS AS (ABS(T)),
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_get_optimize_secondary_range_key
PARTITION BY RANGE COLUMNS(`G`)
SUBPARTITION BY KEY(`K`) SUBPARTITIONS 3
(    PARTITION `p1` VALUES LESS THAN (100),
    PARTITION `p2` VALUES LESS THAN (200),
    PARTITION `p3` VALUES LESS THAN MAXVALUE
);

CREATE TABLEGROUP test_get_optimize SHARDING = 'ADAPTIVE';
CREATE TABLE `test_get_optimize$family_max_version_1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_get_optimize
KV_ATTRIBUTES ='{"Hbase": {"MaxVersions": 1}}'
PARTITION BY KEY(`K`) PARTITIONS 3;

CREATE TABLE `test_get_optimize$family_max_version_default` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_get_optimize
PARTITION BY KEY(`K`) PARTITIONS 3;

CREATE TABLEGROUP test_get_optimize_t SHARDING = 'ADAPTIVE';
CREATE TABLE `test_get_optimize_t$family_max_version_1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_get_optimize_t
KV_ATTRIBUTES ='{"Hbase": {"MaxVersions": 1}}'
PARTITION BY KEY(`K`) PARTITIONS 3;

CREATE TABLE `test_get_optimize_t$family_max_version_default` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_get_optimize_t
PARTITION BY KEY(`K`) PARTITIONS 3;
