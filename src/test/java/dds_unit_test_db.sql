create database if not exists group_00;
create database if not exists group_01;
create database if not exists group_02;
create database if not exists group_03;
create database if not exists group_04;
create database if not exists group_05;
create database if not exists group_06;
create database if not exists group_07;
create database if not exists group_08;
create database if not exists group_09;

create tablegroup test SHARDING = 'ADAPTIVE';

create table if not exists group_00.test$family_00(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_00.test$family_01(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_00.test$family_02(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_00.test$family_03(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_00.test$family_04(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_00.test$family_05(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_00.test$family_06(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_00.test$family_07(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_00.test$family_08(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_00.test$family_09(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';



create table if not exists group_01.test$family_00(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_01.test$family_01(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_01.test$family_02(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_01.test$family_03(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_01.test$family_04(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_01.test$family_05(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_01.test$family_06(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_01.test$family_07(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_01.test$family_08(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_01.test$family_09(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_02.test$family_00(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_02.test$family_01(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_02.test$family_02(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_02.test$family_03(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_02.test$family_04(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_02.test$family_05(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_02.test$family_06(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_02.test$family_07(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_02.test$family_08(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_02.test$family_09(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_03.test$family_00(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_03.test$family_01(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_03.test$family_02(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_03.test$family_03(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_03.test$family_04(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_03.test$family_05(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_03.test$family_06(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_03.test$family_07(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_03.test$family_08(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_03.test$family_09(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_04.test$family_00(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_04.test$family_01(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_04.test$family_02(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_04.test$family_03(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_04.test$family_04(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_04.test$family_05(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_04.test$family_06(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_04.test$family_07(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_04.test$family_08(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_04.test$family_09(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_05.test$family_00(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_05.test$family_01(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_05.test$family_02(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_05.test$family_03(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_05.test$family_04(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_05.test$family_05(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_05.test$family_06(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_05.test$family_07(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_05.test$family_08(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_05.test$family_09(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_06.test$family_00(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_06.test$family_01(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_06.test$family_02(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_06.test$family_03(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_06.test$family_04(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_06.test$family_05(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_06.test$family_06(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_06.test$family_07(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_06.test$family_08(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_06.test$family_09(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_07.test$family_00(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_07.test$family_01(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_07.test$family_02(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_07.test$family_03(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_07.test$family_04(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_07.test$family_05(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_07.test$family_06(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_07.test$family_07(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_07.test$family_08(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_07.test$family_09(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_08.test$family_00(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_08.test$family_01(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_08.test$family_02(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_08.test$family_03(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_08.test$family_04(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_08.test$family_05(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_08.test$family_06(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_08.test$family_07(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_08.test$family_08(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_08.test$family_09(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_09.test$family_00(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_09.test$family_01(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_09.test$family_02(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_09.test$family_03(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_09.test$family_04(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_09.test$family_05(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_09.test$family_06(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_09.test$family_07(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';

create table if not exists group_09.test$family_08(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
create table if not exists group_09.test$family_09(
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) NOT NULL,
    PRIMARY KEY (K, Q, T)
) TABLEGROUP = 'test';
