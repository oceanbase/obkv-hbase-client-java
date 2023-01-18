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
import com.alipay.oceanbase.rpc.property.Property;
import com.google.common.base.Objects;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static com.alipay.oceanbase.hbase.util.Preconditions.checkArgument;
import static org.apache.commons.lang.StringUtils.isNotBlank;

public class ObTableClientManager {

    public static final ConcurrentHashMap<ObTableClientKey, ReentrantLock> OB_TABLE_CLIENT_LOCK     = new ConcurrentHashMap<ObTableClientKey, ReentrantLock>();
    public static final Map<ObTableClientKey, ObTableClient>               OB_TABLE_CLIENT_INSTANCE = new ConcurrentHashMap<ObTableClientKey, ObTableClient>();

    public static ObTableClient getOrCreateObTableClient(Configuration conf)
                                                                            throws IllegalArgumentException,
                                                                            IOException {
        ObTableClientKey obTableClientKey = null;
        if (conf.getBoolean(HBASE_OCEANBASE_ODP_MODE, false)) {
            checkArgument(isNotBlank(conf.get(HBASE_OCEANBASE_ODP_ADDR)), HBASE_OCEANBASE_ODP_ADDR
                                                                          + " is blank");
            checkArgument(conf.getInt(HBASE_OCEANBASE_ODP_PORT, -1) >= 0, HBASE_OCEANBASE_ODP_PORT
                                                                          + " is invalid");
            checkArgument(isNotBlank(conf.get(HBASE_OCEANBASE_DATABASE)), HBASE_OCEANBASE_DATABASE
                                                                          + " is blank");
            obTableClientKey = new ObTableClientKey();
            obTableClientKey.setOdpAddr(conf.get(HBASE_OCEANBASE_ODP_ADDR));
            obTableClientKey.setOdpPort(conf.getInt(HBASE_OCEANBASE_ODP_PORT, -1));
            obTableClientKey.setOdpMode(true);
            obTableClientKey.setDatabase(conf.get(HBASE_OCEANBASE_DATABASE));
        } else {
            checkArgument(isNotBlank(conf.get(HBASE_OCEANBASE_PARAM_URL)),
                HBASE_OCEANBASE_PARAM_URL + " is blank");
            obTableClientKey = new ObTableClientKey();
            obTableClientKey.setParamUrl(conf.get(HBASE_OCEANBASE_PARAM_URL));
            obTableClientKey.setSysUserName(conf.get(HBASE_OCEANBASE_SYS_USER_NAME));
            if (conf.get(HBASE_OCEANBASE_SYS_PASSWORD) == null) {
                obTableClientKey.setSysPassword(Constants.EMPTY_STRING);
            } else {
                obTableClientKey.setSysPassword(conf.get(HBASE_OCEANBASE_SYS_PASSWORD));
            }
        }
        checkArgument(isNotBlank(conf.get(HBASE_OCEANBASE_FULL_USER_NAME)),
            HBASE_OCEANBASE_FULL_USER_NAME + " is blank");
        obTableClientKey.setFullUserName(conf.get(HBASE_OCEANBASE_FULL_USER_NAME));

        if (conf.get(HBASE_OCEANBASE_PASSWORD) == null) {
            obTableClientKey.setPassword(Constants.EMPTY_STRING);
        } else {
            obTableClientKey.setPassword(conf.get(HBASE_OCEANBASE_PASSWORD));
        }

        for (Property property : Property.values()) {
            String value = conf.get(property.getKey());
            if (value != null) {
                obTableClientKey.getProperties().put(property.getKey(), value);
            }
        }

        return getOrCreateObTableClient(obTableClientKey);
    }

    public static ObTableClient getOrCreateObTableClient(ObTableClientKey obTableClientKey)
                                                                                           throws IOException {
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
                        obTableClient.setFullUserName(obTableClientKey.getFullUserName());
                        obTableClient.setPassword(obTableClientKey.getPassword());
                        obTableClient.setOdpMode(obTableClientKey.getOdpMode());
                        obTableClient.setDatabase(obTableClientKey.getDatabase());
                        obTableClient.setRunningMode(ObTableClient.RunningMode.HBASE);
                    } else {
                        obTableClient.setParamURL(obTableClientKey.getParamUrl());
                        obTableClient.setFullUserName(obTableClientKey.getFullUserName());
                        obTableClient.setPassword(obTableClientKey.getPassword());
                        obTableClient.setSysUserName(obTableClientKey.getSysUserName());
                        obTableClient.setSysPassword(obTableClientKey.getSysPassword());
                        obTableClient.setProperties(obTableClientKey.getProperties());
                        obTableClient.setRunningMode(ObTableClient.RunningMode.HBASE);
                    }
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
