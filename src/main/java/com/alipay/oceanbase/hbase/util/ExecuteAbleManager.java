/*-
 * #%L
 * OceanBase Table Hbase Framework
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
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
import com.alipay.oceanbase.rpc.OperationExecuteAble;
import com.alipay.oceanbase.rpc.constant.Constants;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.dds.DdsObTableClient;
import com.google.common.base.Objects;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static com.alipay.oceanbase.hbase.util.Preconditions.checkArgument;
import static com.alipay.oceanbase.hbase.util.TableHBaseLoggerFactory.LCD;
import static org.apache.commons.lang.StringUtils.isNotBlank;

/**
 * @author zhiqi.zzq
 * @since 2018/7/24 下午9:23
 */
public class ExecuteAbleManager {
    private static final Logger                                               logger                       = TableHBaseLoggerFactory
            .getLogger("ExecuteAbleManager");

    public static final ConcurrentHashMap<ObTableClientKey, ReentrantLock>    OB_TABLE_CLIENT_LOCK         = new ConcurrentHashMap<ObTableClientKey, ReentrantLock>();
    public static final Map<ObTableClientKey, ObTableClient>                  OB_TABLE_CLIENT_INSTANCE     = new ConcurrentHashMap<ObTableClientKey, ObTableClient>();

    public static final ConcurrentHashMap<DdsObTableClientKey, ReentrantLock> DDS_OB_TABLE_CLIENT_LOCK     = new ConcurrentHashMap<DdsObTableClientKey, ReentrantLock>();
    public static final Map<DdsObTableClientKey, DdsObTableClient>            DDS_OB_TABLE_CLIENT_INSTANCE = new ConcurrentHashMap<DdsObTableClientKey, DdsObTableClient>();

    /**
     * Get or create the execution handler.
     *
     * Generally , we should not mix the table-specific paramURL mode with the appDataSource mode.
     * If you do mix the methods you should be clearly aware of that you will get the sharding client when
     * the specific table is not declared。
     *
     * @param conf the config
     */
    public static OperationExecuteAble getOrCreateExecuteAble(Configuration conf)
            throws IllegalArgumentException,
            IOException {
        // 1. try to get ObTableClient according to the table-specific paramURL
        String paramUrl = conf.get(HBASE_OCEANBASE_PARAM_URL);
        String fullUserName = conf.get(HBASE_OCEANBASE_FULL_USER_NAME);
        if (isNotBlank(paramUrl) || isNotBlank(fullUserName)) {
            String password = conf.get(HBASE_OCEANBASE_PASSWORD, Constants.EMPTY_STRING);
            return getOrCreateObTableClient(conf, paramUrl, fullUserName, password);
        }

        // 2. otherwise, fall into the DdsObTableClient branch
        return getOrCreateDdsObTableClient(conf);
    }

    /**
     * Get or create ObTableClient according to the obTableClientKey.
     *
     * @param obTableClientKey
     * @return
     * @throws IOException
     */
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
                    obTableClient.setParamURL(obTableClientKey.getParamUrl());
                    obTableClient.setFullUserName(obTableClientKey.getFullUserName());
                    obTableClient.setPassword(obTableClientKey.getPassword());
                    obTableClient.setProperties(obTableClientKey.getProperties());
                    obTableClient.setRunningMode(ObTableClient.RunningMode.HBASE);
                    obTableClient.init();
                    OB_TABLE_CLIENT_INSTANCE.put(obTableClientKey, obTableClient);
                    if (logger.isInfoEnabled()) {
                        logger.info("create ObTableClient success with {}", obTableClientKey);
                    }
                }
            } catch (Exception e) {
                logger.error(LCD.convert("01-00009"), obTableClientKey, e);
                throw new IOException("create ObTableClient error with " + obTableClientKey, e);
            } finally {
                lock.unlock();
            }
        }
        return OB_TABLE_CLIENT_INSTANCE.get(obTableClientKey);
    }

    /**
     * Get or create ObTableClient according to paramUrl info.
     *
     * @param conf
     * @param paramUrl
     * @param fullUserName
     * @param password
     * @return
     * @throws IOException
     */
    public static ObTableClient getOrCreateObTableClient(Configuration conf, String paramUrl,
                                                         String fullUserName, String password)
            throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("try to getOrCreateObTableClient with paramUrl:" + paramUrl
                    + ", fullUserName:" + fullUserName);
        }
        checkArgument(isNotBlank(paramUrl), HBASE_OCEANBASE_PARAM_URL + " is blank");
        checkArgument(isNotBlank(fullUserName), HBASE_OCEANBASE_FULL_USER_NAME + " is blank");

        ObTableClientKey obTableClientKey = new ObTableClientKey();
        obTableClientKey.setParamUrl(paramUrl);
        obTableClientKey.setFullUserName(fullUserName);
        obTableClientKey.setPassword(password);
        for (Property property : Property.values()) {
            String value = conf.get(property.getKey());
            if (value != null) {
                obTableClientKey.getProperties().put(property.getKey(), value);
            }
        }
        return getOrCreateObTableClient(obTableClientKey);
    }

    /**
     * ObTableClientKey consists of the triplet: paramUrl, fullUserName and password.
     */
    public static class ObTableClientKey {

        private String     paramUrl;
        private String     fullUserName;
        private String     password;

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
            return Objects.equal(paramUrl, that.paramUrl)
                    && Objects.equal(fullUserName, that.fullUserName)
                    && Objects.equal(password, that.password);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(paramUrl, fullUserName, password);
        }

        @Override
        public String toString() {
            return "ObTableClientKey{" + "paramUrl='" + paramUrl + '\'' + ", fullUserName='"
                    + fullUserName + '\'' + ", properties=" + properties + '}';
        }
    }

    /**
     * DdsObTableClientKey consists of the appDataSource triplet: appName, appDsName and version.
     */
    public static class DdsObTableClientKey {

        private String     appName;
        private String     appDsName;
        private String     version;

        private Properties properties = new Properties();

        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        public String getAppDsName() {
            return appDsName;
        }

        public void setAppDsName(String appDsName) {
            this.appDsName = appDsName;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
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
            if (!(o instanceof DdsObTableClientKey))
                return false;
            DdsObTableClientKey that = (DdsObTableClientKey) o;
            return Objects.equal(getAppName(), that.getAppName())
                    && Objects.equal(getAppDsName(), that.getAppDsName())
                    && Objects.equal(getVersion(), that.getVersion());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getAppName(), getAppDsName(), getVersion());
        }

        @Override
        public String toString() {
            return "DdsObTableClientKey{" + "appName='" + appName + '\'' + ", appDsName='"
                    + appDsName + '\'' + ", version='" + version + '\'' + ", properties="
                    + properties + '}';
        }
    }

    /**
     * Get or create DdsObTableClient according to ddsObTableClientKey.
     *
     * @param ddsObTableClientKey
     * @return
     * @throws IOException
     */
    public static DdsObTableClient getOrCreateDdsObTableClient(DdsObTableClientKey ddsObTableClientKey)
            throws IOException {
        if (DDS_OB_TABLE_CLIENT_INSTANCE.get(ddsObTableClientKey) == null) {
            ReentrantLock tmp = new ReentrantLock();
            ReentrantLock lock = DDS_OB_TABLE_CLIENT_LOCK.putIfAbsent(ddsObTableClientKey, tmp);
            lock = lock == null ? tmp : lock;
            lock.lock();
            try {
                if (DDS_OB_TABLE_CLIENT_INSTANCE.get(ddsObTableClientKey) == null) {
                    DdsObTableClient ddsObTableClient = new DdsObTableClient();
                    ddsObTableClient.setAppName(ddsObTableClientKey.getAppName());
                    ddsObTableClient.setAppDsName(ddsObTableClientKey.getAppDsName());
                    ddsObTableClient.setVersion(ddsObTableClientKey.getVersion());
                    ddsObTableClient.setRunningMode(ObTableClient.RunningMode.HBASE);
                    ddsObTableClient.setTableClientProperty(ddsObTableClientKey.getProperties());
                    ddsObTableClient.init();
                    DDS_OB_TABLE_CLIENT_INSTANCE.put(ddsObTableClientKey, ddsObTableClient);
                    logger.info("create DdsObTableClient success with {}", ddsObTableClientKey);
                }
            } catch (Exception e) {
                logger.error(LCD.convert("01-00010"), ddsObTableClientKey, e);
                throw new IOException("create DdsObTableClient error with " + ddsObTableClientKey,
                        e);
            } finally {
                lock.unlock();
            }
        }
        return DDS_OB_TABLE_CLIENT_INSTANCE.get(ddsObTableClientKey);
    }

    /**
     *  Get or create DdsObTableClient according to appDataSource.
     *
     * @param conf
     * @return
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public static DdsObTableClient getOrCreateDdsObTableClient(Configuration conf)
            throws IllegalArgumentException,
            IOException {
        String appName = conf.get(HBASE_OCEANBASE_DDS_APP_NAME);
        String appDsName = conf.get(HBASE_OCEANBASE_DDS_APP_DS_NAME);
        String version = conf.get(HBASE_OCEANBASE_DDS_VERSION);
        if (logger.isDebugEnabled()) {
            logger.debug("try to getOrCreateDdsObTableClient with appName:" + appName
                    + ", appDsName:" + appDsName + ", version:" + version);
        }
        checkArgument(isNotBlank(appName), HBASE_OCEANBASE_DDS_APP_NAME + " is blank");
        checkArgument(isNotBlank(appDsName), HBASE_OCEANBASE_DDS_APP_DS_NAME + " is blank");
        checkArgument(isNotBlank(version), HBASE_OCEANBASE_DDS_VERSION + " is blank");

        DdsObTableClientKey ddsObTableClientKey = new DdsObTableClientKey();
        ddsObTableClientKey.setAppName(appName);
        ddsObTableClientKey.setAppDsName(appDsName);
        ddsObTableClientKey.setVersion(version);

        for (Property property : Property.values()) {
            String value = conf.get(property.getKey());
            if (value != null) {
                ddsObTableClientKey.getProperties().put(property.getKey(), value);
            }
        }
        return getOrCreateDdsObTableClient(ddsObTableClientKey);
    }
}