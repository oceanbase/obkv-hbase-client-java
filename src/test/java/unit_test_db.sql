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

