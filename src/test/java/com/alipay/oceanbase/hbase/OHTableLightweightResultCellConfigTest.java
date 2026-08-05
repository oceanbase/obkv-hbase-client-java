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

package com.alipay.oceanbase.hbase;

import com.alipay.oceanbase.rpc.ObTableClient;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_GET_LIGHTWEIGHT_RESULT_CELL_DEFAULT;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_GET_LIGHTWEIGHT_RESULT_CELL_ENABLED;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_SCAN_LIGHTWEIGHT_RESULT_CELL_DEFAULT;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_SCAN_LIGHTWEIGHT_RESULT_CELL_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class OHTableLightweightResultCellConfigTest {

    private ExecutorService executorService;

    @Before
    public void setUp() {
        executorService = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() {
        executorService.shutdownNow();
    }

    @Test
    public void testPointAndScanLightweightCellSwitchesAreIndependent() throws Exception {
        OHTable pointOnly = new OHTable(Bytes.toBytes("test"), mock(ObTableClient.class),
            executorService, true, false);
        assertTrue(getBooleanField(pointOnly, "getLightweightResultCellEnabled"));
        assertFalse(getBooleanField(pointOnly, "scanLightweightResultCellEnabled"));

        OHTable scanOnly = new OHTable(Bytes.toBytes("test"), mock(ObTableClient.class),
            executorService, false, true);
        assertFalse(getBooleanField(scanOnly, "getLightweightResultCellEnabled"));
        assertTrue(getBooleanField(scanOnly, "scanLightweightResultCellEnabled"));
    }

    @Test
    public void testInternalConstructorUsesScanLightweightCellDefault() throws Exception {
        OHTable table = new OHTable(Bytes.toBytes("test"), mock(ObTableClient.class),
            executorService, true);

        assertTrue(getBooleanField(table, "getLightweightResultCellEnabled"));
        assertTrue(getBooleanField(table, "scanLightweightResultCellEnabled"));
    }

    @Test
    public void testLightweightCellConfigurationNamesAndDefaults() {
        assertEquals("hbase.htable.get.lightweight.result.cell.enabled",
            HBASE_HTABLE_GET_LIGHTWEIGHT_RESULT_CELL_ENABLED);
        assertEquals("hbase.htable.scan.lightweight.result.cell.enabled",
            HBASE_HTABLE_SCAN_LIGHTWEIGHT_RESULT_CELL_ENABLED);
        assertTrue(HBASE_HTABLE_GET_LIGHTWEIGHT_RESULT_CELL_DEFAULT);
        assertTrue(HBASE_HTABLE_SCAN_LIGHTWEIGHT_RESULT_CELL_DEFAULT);
    }

    private static boolean getBooleanField(OHTable table, String fieldName) throws Exception {
        Field field = OHTable.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getBoolean(table);
    }

}
