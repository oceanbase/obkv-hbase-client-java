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