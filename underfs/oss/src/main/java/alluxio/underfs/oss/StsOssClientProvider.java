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
import com.google.common.annotations.VisibleForTesting;
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
  private static final String ACCESS_KEY_ID = "AccessKeyId";
  private static final String ACCESS_KEY_SECRET = "AccessKeySecret";
  private static final String SECURITY_TOKEN = "SecurityToken";
  private static final String EXPIRATION = "Expiration";

  private volatile OSS mOssClient = null;
  private long mStsTokenExpiration = 0;
  private final String mEcsMetadataServiceUrl;
  private final long mTokenTimeoutMs;
  private final UnderFileSystemConfiguration mOssConf;
  private final ScheduledExecutorService mRefreshOssClientScheduledThread;
  private OSSClientBuilder mOssClientBuilder = new OSSClientBuilder();

  /**
   * Constructs a new instance of {@link StsOssClientProvider}.
   * @param ossConfiguration {@link UnderFileSystemConfiguration} for OSS
   */
  public StsOssClientProvider(UnderFileSystemConfiguration ossConfiguration) {
    mOssConf = ossConfiguration;
    mEcsMetadataServiceUrl = ossConfiguration.getString(
        PropertyKey.UNDERFS_OSS_STS_ECS_METADATA_SERVICE_ENDPOINT);
    mTokenTimeoutMs = ossConfiguration.getMs(PropertyKey.UNDERFS_OSS_STS_TOKEN_REFRESH_INTERVAL_MS);

    mRefreshOssClientScheduledThread = Executors.newSingleThreadScheduledExecutor(
        ThreadFactoryUtils.build("refresh_oss_client-%d", false));
    mRefreshOssClientScheduledThread.scheduleAtFixedRate(() -> {
      try {
        createOrRefreshOssStsClient(mOssConf);
      } catch (Exception e) {
        //retry it
        LOG.warn("exception when refreshing OSS client access token", e);
      }
    }, 0, 60000, TimeUnit.MILLISECONDS);
  }

  /**
   * Init {@link StsOssClientProvider}.
   * @throws IOException if failed to init OSS Client
   */
  public void init() throws IOException {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(
        BASE_SLEEP_TIME_MS, MAX_SLEEP_MS, MAX_RETRIES);
    IOException lastException = null;
    while (retryPolicy.attempt()) {
      try {
        createOrRefreshOssStsClient(mOssConf);
        lastException = null;
        break;
      } catch (IOException e) {
        LOG.warn("init oss client failed! has retried {} times", retryPolicy.getAttemptCount(), e);
        lastException = e;
      }
    }
    if (lastException != null) {
      LOG.error("init oss client failed.", lastException);
      throw lastException;
    }
  }

  /**
   * Create Or Refresh the STS OSS client.
   * @param ossConfiguration OSS {@link UnderFileSystemConfiguration}
   * @throws IOException if failed to create or refresh OSS client
   */
  protected void createOrRefreshOssStsClient(UnderFileSystemConfiguration ossConfiguration)
      throws IOException {
    ClientBuilderConfiguration ossClientConf =
        OSSUnderFileSystem.initializeOSSClientConfig(ossConfiguration);
    doCreateOrRefreshStsOssClient(ossConfiguration, ossClientConf);
  }

  boolean tokenWillExpiredAfter(long after) {
    return mStsTokenExpiration - System.currentTimeMillis() <= after;
  }

  private void doCreateOrRefreshStsOssClient(
      UnderFileSystemConfiguration ossConfiguration,
      ClientBuilderConfiguration clientConfiguration) throws IOException {
    if (tokenWillExpiredAfter(mTokenTimeoutMs)) {
      String ecsRamRole = ossConfiguration.getString(PropertyKey.UNDERFS_OSS_ECS_RAM_ROLE);
      String fullECSMetaDataServiceUrl = mEcsMetadataServiceUrl + ecsRamRole;
      String jsonStringResponse = HttpUtils.get(fullECSMetaDataServiceUrl, ECS_META_GET_TIMEOUT);

      JsonObject jsonObject = new Gson().fromJson(jsonStringResponse, JsonObject.class);
      String accessKeyId = jsonObject.get(ACCESS_KEY_ID).getAsString();
      String accessKeySecret = jsonObject.get(ACCESS_KEY_SECRET).getAsString();
      String securityToken = jsonObject.get(SECURITY_TOKEN).getAsString();
      mStsTokenExpiration =
          convertStringToDate(jsonObject.get(EXPIRATION).getAsString()).getTime();

      if (null == mOssClient) {
        mOssClient = mOssClientBuilder.build(
            ossConfiguration.getString(PropertyKey.OSS_ENDPOINT_KEY),
            accessKeyId, accessKeySecret, securityToken,
            clientConfiguration);
      } else {
        mOssClient.switchCredentials((new DefaultCredentials(
            accessKeyId, accessKeySecret, securityToken)));
      }
      LOG.debug("oss sts client create success, expiration = {}", mStsTokenExpiration);
    }
  }

  /**
   * Returns the STS OSS client.
   * @return oss client
   */
  public OSS getOSSClient() {
    return mOssClient;
  }

  private Date convertStringToDate(String dateString) throws IOException {
    TimeZone zeroTimeZone = TimeZone.getTimeZone("ETC/GMT-0");
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    sdf.setTimeZone(zeroTimeZone);
    Date date = null;
    try {
      date = sdf.parse(dateString);
    } catch (ParseException e) {
      throw new IOException(String.format("failed to parse date: %s", dateString), e);
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

  @VisibleForTesting
  protected void setOssClientBuilder(OSSClientBuilder ossClientBuilder) {
    mOssClientBuilder = ossClientBuilder;
  }
}
