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

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.constant.Constants;
import com.google.common.base.Objects;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionConfiguration;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static com.alipay.oceanbase.hbase.util.Preconditions.checkArgument;
import static org.apache.commons.lang.StringUtils.isNotBlank;

@InterfaceAudience.Private
public class ObTableClientManager {

    public static final ConcurrentHashMap<ObTableClientKey, ReentrantLock> OB_TABLE_CLIENT_LOCK     = new ConcurrentHashMap<ObTableClientKey, ReentrantLock>();
    public static final Map<ObTableClientKey, ObTableClient>               OB_TABLE_CLIENT_INSTANCE = new ConcurrentHashMap<ObTableClientKey, ObTableClient>();

    public static ObTableClient getOrCreateObTableClient(OHConnectionConfiguration connectionConfig)
                                                                                                    throws IllegalArgumentException,
                                                                                                    IOException {
        ObTableClientKey obTableClientKey = null;
        if (connectionConfig.isOdpMode()) {
            checkArgument(isNotBlank(connectionConfig.getOdpAddr()), HBASE_OCEANBASE_ODP_ADDR
                                                                     + " is blank");
            checkArgument(connectionConfig.getOdpPort() >= 0, HBASE_OCEANBASE_ODP_PORT
                                                              + " is invalid");
            checkArgument(isNotBlank(connectionConfig.getDatabase()), HBASE_OCEANBASE_DATABASE
                                                                      + " is blank");
            obTableClientKey = new ObTableClientKey();
            obTableClientKey.setOdpAddr(connectionConfig.getOdpAddr());
            obTableClientKey.setOdpPort(connectionConfig.getOdpPort());
            obTableClientKey.setOdpMode(true);
            obTableClientKey.setDatabase(connectionConfig.getDatabase());
        } else {
            checkArgument(isNotBlank(connectionConfig.getParamUrl()), HBASE_OCEANBASE_PARAM_URL
                                                                      + " is blank");
            obTableClientKey = new ObTableClientKey();
            String paramUrl = connectionConfig.getParamUrl();
            if (!paramUrl.contains("database")) {
                paramUrl += "&database=default";
            }
            obTableClientKey.setParamUrl(paramUrl);
            obTableClientKey.setSysUserName(connectionConfig.getSysUsername());
            if (connectionConfig.getSysPassword() == null) {
                obTableClientKey.setSysPassword(Constants.EMPTY_STRING);
            } else {
                obTableClientKey.setSysPassword(connectionConfig.getSysPassword());
            }
        }
        checkArgument(isNotBlank(connectionConfig.getFullUsername()),
            HBASE_OCEANBASE_FULL_USER_NAME + " is blank");
        obTableClientKey.setFullUserName(connectionConfig.getFullUsername());

        if (connectionConfig.getPassword() == null) {
            obTableClientKey.setPassword(Constants.EMPTY_STRING);
        } else {
            obTableClientKey.setPassword(connectionConfig.getPassword());
        }

        for (Map.Entry<Object, Object> property : connectionConfig.getProperties().entrySet()) {
            obTableClientKey.getProperties().put(property.getKey(), property.getValue());
        }

        return getOrCreateObTableClient(obTableClientKey, connectionConfig.getRpcConnectTimeout());
    }

    public static ObTableClient getOrCreateObTableClient(ObTableClientKey obTableClientKey,
                                                         int rpcConnectTimeout) throws IOException {
        if (OB_TABLE_CLIENT_INSTANCE.get(obTableClientKey) == null) {
            ReentrantLock tmp = new ReentrantLock();
            ReentrantLock lock = OB_TABLE_CLIENT_LOCK.putIfAbsent(obTableClientKey, tmp);
            lock = lock == null ? tmp : lock;
            lock.lock();
            try {
                if (OB_TABLE_CLIENT_INSTANCE.get(obTableClientKey) == null) {
                    ObTableClient obTableClient = new ObTableClient();
                    if (obTableClientKey.getOdpMode()) {
                        obTableClient.setOdpAddr(obTableClientKey.getOdpAddr());
                        obTableClient.setOdpPort(obTableClientKey.getOdpPort());
                        obTableClient.setOdpMode(obTableClientKey.getOdpMode());
                        obTableClient.setDatabase(obTableClientKey.getDatabase());
                        obTableClient.setProperties(obTableClientKey.getProperties());
                        obTableClient.setRunningMode(ObTableClient.RunningMode.HBASE);
                    } else {
                        obTableClient.setParamURL(obTableClientKey.getParamUrl());
                        obTableClient.setSysUserName(obTableClientKey.getSysUserName());
                        obTableClient.setSysPassword(obTableClientKey.getSysPassword());
                        obTableClient.setProperties(obTableClientKey.getProperties());
                        obTableClient.setRunningMode(ObTableClient.RunningMode.HBASE);
                    }
                    obTableClient.setFullUserName(obTableClientKey.getFullUserName());
                    obTableClient.setPassword(obTableClientKey.getPassword());
                    obTableClient.setRpcConnectTimeout(rpcConnectTimeout);
                    obTableClient.init();
                    OB_TABLE_CLIENT_INSTANCE.put(obTableClientKey, obTableClient);
                }
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                lock.unlock();
            }
        }
        return OB_TABLE_CLIENT_INSTANCE.get(obTableClientKey);
    }

    public static void clear() throws IOException {
        try {
            for (Map.Entry<ObTableClientKey, ObTableClient> pair : OB_TABLE_CLIENT_INSTANCE.entrySet()) {
                pair.getValue().close();
            }
        }
        catch (Exception e) {
            throw new IOException("fail to close tableClient" , e);
        }
        OB_TABLE_CLIENT_INSTANCE.clear();
    }

    public static class ObTableClientKey {
        private String     paramUrl;
        private String     fullUserName;
        private String     password;
        private String     sysUserName;
        private String     sysPassword;
        private String     odpAddr;
        private int        odpPort;
        private String     database;
        private boolean    odpMode    = false;
        private Properties properties = new Properties();

        public String getParamUrl() {
            return paramUrl;
        }

        public void setParamUrl(String paramUrl) {
            this.paramUrl = paramUrl;
        }

        public String getFullUserName() {
            return fullUserName;
        }

        public void setFullUserName(String fullUserName) {
            this.fullUserName = fullUserName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getSysUserName() {
            return sysUserName;
        }

        public void setSysUserName(String sysUserName) {
            this.sysUserName = sysUserName;
        }

        public String getSysPassword() {
            return sysPassword;
        }

        public void setSysPassword(String sysPassword) {
            this.sysPassword = sysPassword;
        }

        public String getOdpAddr() {
            return odpAddr;
        }

        public void setOdpAddr(String odpAddr) {
            this.odpAddr = odpAddr;
        }

        public int getOdpPort() {
            return odpPort;
        }

        public void setOdpPort(int odpPort) {
            this.odpPort = odpPort;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public boolean getOdpMode() {
            return odpMode;
        }

        public void setOdpMode(boolean odpMode) {
            this.odpMode = odpMode;
        }

        public Properties getProperties() {
            return properties;
        }

        public void setProperties(Properties properties) {
            this.properties = properties;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            ObTableClientKey that = (ObTableClientKey) o;
            boolean ans = false;
            if (odpMode) {
                ans = Objects.equal(fullUserName, that.fullUserName)
                      && Objects.equal(password, that.password)
                      && Objects.equal(odpAddr, that.odpAddr) && odpPort == that.odpPort
                      && Objects.equal(database, that.database);

            } else {
                ans = Objects.equal(paramUrl, that.paramUrl)
                      && Objects.equal(fullUserName, that.fullUserName)
                      && Objects.equal(password, that.password)
                      && Objects.equal(sysUserName, that.sysUserName)
                      && Objects.equal(sysPassword, that.sysPassword);
            }
            return ans;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(paramUrl, fullUserName, password, sysUserName, sysPassword);
        }
    }
}
