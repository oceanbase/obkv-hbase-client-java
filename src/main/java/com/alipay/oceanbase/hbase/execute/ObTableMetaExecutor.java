/*-
 * #%L
 * com.oceanbase:obkv-hbase-client
 * %%
 * Copyright (C) 2022 - 2025 OceanBase Group
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

package com.alipay.oceanbase.hbase.execute;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;

import java.io.IOException;

public interface ObTableMetaExecutor<T> {
    /**
     * 执行元数据请求
     * @param request 元数据请求
     * @return 解析后的元数据对象
     * @throws IOException 如果执行失败或解析失败
     */
    T execute(ObTableClient client, ObTableMetaRequest request) throws IOException;

    /**
     * 解析元数据响应, 用户需要重写
     * @param response 元数据响应
     * @return 解析后的元数据对象
     * @throws IOException 如果解析失败
     */
    T parse(ObTableMetaResponse response) throws IOException;

    /**
     * 获取元信息类型, 用户需要重写
     * @return 元信息类型
     */
    ObTableRpcMetaType getMetaType() throws IOException;
}
