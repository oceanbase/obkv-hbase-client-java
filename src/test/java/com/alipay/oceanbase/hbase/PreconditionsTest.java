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

import com.alipay.oceanbase.hbase.util.Preconditions;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PreconditionsTest {
    @Test
    public void test() {
        try {
            Preconditions.checkArgument(false);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            Preconditions.checkArgument(false, "error");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            Preconditions.checkArgument(false, "test {}", "error");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            Preconditions.checkState(false);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            Preconditions.checkState(false, "error");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            Preconditions.checkState(false, "test {}", "error");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            Preconditions.checkNotNull("test");
            Preconditions.checkNotNull(null);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            Preconditions.checkNotNull("test", "error");
            Preconditions.checkNotNull(null, "error");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            Preconditions.checkNotNull("test", "test {}", "error");
            Preconditions.checkNotNull(null, "test {}", "error");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            Preconditions.checkElementIndex(0, 10);
            Preconditions.checkElementIndex(11, 10);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            Preconditions.checkPositionIndex(0, 10);
            Preconditions.checkPositionIndex(11, 10);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            Preconditions.checkPositionIndexes(0, 10, 100);
            Preconditions.checkPositionIndexes(0, 100, 10);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }
    }
}
