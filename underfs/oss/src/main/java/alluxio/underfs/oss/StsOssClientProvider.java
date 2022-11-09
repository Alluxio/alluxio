/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.oss;

import alluxio.conf.PropertyKey;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.HttpUtils;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.DefaultCredentials;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * STS client provider for Aliyun OSS.
 */
public class StsOssClientProvider implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(StsOssClientProvider.class);

  private static final int ECS_META_GET_TIMEOUT = 10000;
  private static final int BASE_SLEEP_TIME_MS = 1000;
  private static final int MAX_SLEEP_MS = 3000;
  private static final int MAX_RETRIES = 5;

  private volatile OSS mOssClient = null;
  private Date mStsTokenExpiration = null;

  public static final String ECS_METADATA_SERVICE =
      "http://100.100.100.200/latest/meta-data/ram/security-credentials/";

  private static final int IN_TOKEN_EXPIRED_MS = 1800000;
  private static final String ACCESS_KEY_ID = "AccessKeyId";
  private static final String ACCESS_KEY_SECRET = "AccessKeySecret";
  private static final String SECURITY_TOKEN = "SecurityToken";
  private static final String EXPIRATION = "Expiration";

  private UnderFileSystemConfiguration mOssConf;
  private final ScheduledExecutorService mRefreshOssClientScheduledThread;

  /**
   * Constructs a new instance of {@link StsOssClientProvider}.
   * @param ossConfiguration {@link UnderFileSystemConfiguration} for OSS
   * @throws IOException if failed to init OSS STS client
   */
  public StsOssClientProvider(UnderFileSystemConfiguration ossConfiguration) throws IOException {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(
        BASE_SLEEP_TIME_MS, MAX_SLEEP_MS, MAX_RETRIES);
    IOException lastException = null;
    while (retryPolicy.attempt()) {
      try {
        initializeOssClient(ossConfiguration);
        lastException = null;
        break;
      } catch (IOException e) {
        LOG.warn("init oss client failed! has retry {} times", retryPolicy.getAttemptCount(), e);
        lastException = e;
      }
    }
    if (lastException != null) {
      throw lastException;
    }
    mRefreshOssClientScheduledThread = Executors.newSingleThreadScheduledExecutor(
        ThreadFactoryUtils.build("refresh_oss_client-%d", false));
    mRefreshOssClientScheduledThread.scheduleAtFixedRate(() -> {
      try {
        if (null != mOssConf) {
          refreshOssStsClient(mOssConf);
        }
      } catch (Exception e) {
        //retry it
        LOG.warn("throw exception when clear meta data cache", e);
      }
    }, 0, 60000, TimeUnit.MILLISECONDS);
  }

  protected void refreshOssStsClient(UnderFileSystemConfiguration ossConfiguration)
      throws IOException {
    ClientBuilderConfiguration ossClientConf = getClientBuilderConfiguration(ossConfiguration);
    createOrRefreshStsOssClient(ossConfiguration, ossClientConf);
  }

  private ClientBuilderConfiguration getClientBuilderConfiguration(
      UnderFileSystemConfiguration ossConfiguration) {
    ClientBuilderConfiguration ossClientConf = new ClientBuilderConfiguration();
    ossClientConf.setMaxConnections(ossConfiguration.getInt(PropertyKey.UNDERFS_OSS_CONNECT_MAX));
    ossClientConf.setMaxErrorRetry(ossConfiguration.getInt(PropertyKey.UNDERFS_OSS_RETRY_MAX));
    ossClientConf.setConnectionTimeout(
        (int) ossConfiguration.getMs(PropertyKey.UNDERFS_OSS_CONNECT_TIMEOUT));
    ossClientConf.setSocketTimeout(
        (int) ossConfiguration.getMs(PropertyKey.UNDERFS_OSS_SOCKET_TIMEOUT));
    ossClientConf.setSupportCname(false);
    ossClientConf.setCrcCheckEnabled(true);
    return ossClientConf;
  }

  /**
   * Init the STS OSS client.
   * @param ossConfiguration OSS {@link UnderFileSystemConfiguration}
   * @throws IOException if failed to init OSS client
   */
  public void initializeOssClient(UnderFileSystemConfiguration ossConfiguration)
      throws IOException {
    mOssConf = ossConfiguration;
    if (null != mOssClient) {
      return;
    }

    ClientBuilderConfiguration clientConf = getClientBuilderConfiguration(ossConfiguration);
    createOrRefreshStsOssClient(ossConfiguration, clientConf);

    LOG.info("init ossClient success : {}", mOssClient.toString());
  }

  boolean isStsTokenExpired() {
    boolean expired = true;
    Date now = convertLongToDate(System.currentTimeMillis());
    if (null != mStsTokenExpiration) {
      if (mStsTokenExpiration.after(now)) {
        expired = false;
      }
    }
    return expired;
  }

  boolean isTokenWillExpired() {
    boolean in = true;
    Date now = convertLongToDate(System.currentTimeMillis());
    long millisecond = mStsTokenExpiration.getTime() - now.getTime();
    if (millisecond >= IN_TOKEN_EXPIRED_MS) {
      in = false;
    }
    return in;
  }

  private void createOrRefreshStsOssClient(
      UnderFileSystemConfiguration ossConfiguration,
      ClientBuilderConfiguration clientConfiguration) throws IOException {
    if (isStsTokenExpired() || isTokenWillExpired()) {
      try {
        String ecsRamRole = ossConfiguration.getString(PropertyKey.UNDERFS_OSS_ECS_RAM_ROLE);
        String fullECSMetaDataServiceUrl = ECS_METADATA_SERVICE + ecsRamRole;
        String jsonStringResponse = HttpUtils.get(fullECSMetaDataServiceUrl, ECS_META_GET_TIMEOUT);

        JsonObject jsonObject = new Gson().fromJson(jsonStringResponse, JsonObject.class);
        String accessKeyId = jsonObject.get(ACCESS_KEY_ID).getAsString();
        String accessKeySecret = jsonObject.get(ACCESS_KEY_SECRET).getAsString();
        String securityToken = jsonObject.get(SECURITY_TOKEN).getAsString();
        mStsTokenExpiration = convertStringToDate(jsonObject.get(EXPIRATION).getAsString());

        if (null == mOssClient) {
          mOssClient = new OSSClientBuilder().build(
              ossConfiguration.getString(PropertyKey.OSS_ENDPOINT_KEY),
              accessKeyId, accessKeySecret, securityToken,
              clientConfiguration);
        } else {
          mOssClient.switchCredentials((new DefaultCredentials(
              accessKeyId, accessKeySecret, securityToken)));
        }
        LOG.info("oss sts client create success {} {} {}",
            mOssClient, securityToken, mStsTokenExpiration);
      } catch (IOException e) {
        LOG.error("create stsOssClient exception", e);
        throw new IOException("create stsOssClient exception", e);
      }
    }
  }

  /**
   * Returns the STS OSS client.
   * @return oss client
   */
  public OSS getOSSClient() {
    return mOssClient;
  }

  private Date convertLongToDate(long timeMs) {
    TimeZone zeroTimeZone = TimeZone.getTimeZone("ETC/GMT-0");
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    sdf.setTimeZone(zeroTimeZone);
    Date date = null;
    try {
      date = sdf.parse(sdf.format(new Date(timeMs)));
    } catch (ParseException e) {
      LOG.error("convert String to Date type error", e);
    }
    return date;
  }

  private Date convertStringToDate(String dateString) {
    TimeZone zeroTimeZone = TimeZone.getTimeZone("ETC/GMT-0");
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    sdf.setTimeZone(zeroTimeZone);
    Date date = null;
    try {
      date = sdf.parse(dateString);
    } catch (ParseException e) {
      LOG.error("convert String to Date type error", e);
    }
    return date;
  }

  @Override
  public void close() throws IOException {
    if (null != mRefreshOssClientScheduledThread) {
      mRefreshOssClientScheduledThread.shutdown();
    }
    if (null != mOssClient) {
      mOssClient.shutdown();
      mOssClient = null;
    }
  }
}
