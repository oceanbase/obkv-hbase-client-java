package com.alipay.oceanbase.hbase;

import com.alipay.oceanbase.hbase.util.NativeHBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;

public class NativeHBaseTest extends HTableTestBase {

    static Admin     admin;
    static TableName tableName1 = TableName.valueOf("test");
    static TableName tableName2 = TableName.valueOf("test_multi_cf");

    static {
        try {
            admin = NativeHBaseUtil.getAdmin();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setup() throws IOException {
        hTable = NativeHBaseUtil.getTable(tableName1);
        multiCfHTable = NativeHBaseUtil.getTable(tableName2);
    }

    @Before
    public void cleanData() throws IOException {
        admin.disableTable(tableName1);
        admin.disableTable(tableName2);
        admin.truncateTable(tableName1, true);
        admin.truncateTable(tableName2, true);
    }

    @AfterClass
    public static void finish() throws IOException {
        hTable.close();
        multiCfHTable.close();
    }

}
