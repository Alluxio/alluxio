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

package alluxio.check;

import alluxio.ProjectConstants;
import alluxio.util.EnvironmentUtils;
import alluxio.util.FeatureUtils;

import com.amazonaws.util.EC2MetadataUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Check for updates.
 */
@ThreadSafe
public final class UpdateCheck {
  static final String BACKUP_DELEGATION_KEY = "backupDelegation";
  static final String CFT_KEY = "cft";
  static final String DAILY_BACKUP_KEY = "dailyBackup";
  static final String DOCKER_KEY = "docker";
  static final String EC2_KEY = "ec2";
  static final String EMBEDDED_KEY = "embedded";
  static final String EMR_KEY = "emr";
  static final String GCE_KEY = "gce";
  static final String KUBERNETES_KEY = "kubernetes";
  static final String MASTER_AUDIT_LOG_KEY = "masterAuditLog";
  static final String PERSIST_BLACK_LIST_KEY = "persistBlackList";
  static final String PAGE_STORE_KEY = "pageStore";
  static final String PRODUCT_CODE_KEY = "ProductCode:";
  static final String ROCKS_KEY = "rocks";
  static final String UNSAFE_PERSIST_KEY = "unsafePersist";
  static final String ZOOKEEPER_KEY = "zookeeper";

  /**
   * @param id the id of the current Alluxio identity (e.g. cluster id, instance id)
   * @param additionalInfo additional information to send
   * @param connectionRequestTimeout the connection request timeout for the HTTP request in ms
   * @param connectTimeout the connection timeout for the HTTP request in ms
   * @param socketTimeout the socket timeout for the HTTP request in ms
   * @return the latest Alluxio version string
   */
  public static String getLatestVersion(String id, List<String> additionalInfo,
      long connectionRequestTimeout,
      long connectTimeout, long socketTimeout) throws IOException {
    // Create the GET request.
    Joiner joiner = Joiner.on("/");
    String path = joiner.join("v0", "version");
    String url = new URL(new URL(ProjectConstants.UPDATE_CHECK_HOST), path).toString();

    HttpGet post = new HttpGet(url);
    post.setHeader("User-Agent", getUserAgentString(id, additionalInfo));
    post.setHeader("Authorization", "Basic " + ProjectConstants.UPDATE_CHECK_MAGIC_NUMBER);

    // Fire off the version check request.
    HttpClient client = HttpClientBuilder.create()
        .setDefaultRequestConfig(
            RequestConfig.custom()
                .setConnectionRequestTimeout((int) connectionRequestTimeout)
                .setConnectTimeout((int) connectTimeout)
                .setSocketTimeout((int) socketTimeout)
                .build())
        .build();
    HttpResponse response = client.execute(post);

    // Check the response code.
    int responseCode = response.getStatusLine().getStatusCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      throw new IOException("Update check request failed with code: " + responseCode);
    }

    return EntityUtils.toString(response.getEntity(), "UTF-8");
  }

  /**
   * @param clusterID the cluster ID
   * @param additionalInfo additional information to add to result string
   * @return a string representation of the user's environment in the format
   *         "Alluxio/{ALLUXIO_VERSION} (valueA; valueB)"
   */
  @VisibleForTesting
  public static String getUserAgentString(String clusterID, List<String> additionalInfo) {
    List<String> info = new ArrayList<>();
    info.add(clusterID);
    addUserAgentEnvironments(info);
    addUserAgentFeatures(info);
    info.addAll(additionalInfo);
    return String.format("Alluxio/%s (%s)", ProjectConstants.VERSION,
        Joiner.on("; ").skipNulls().join(info));
  }

  /**
   * Adds the information of user environment to given list.
   *
   * @param info the list to add info to
   */
  @VisibleForTesting
  public static void addUserAgentEnvironments(List<String> info) {
    if (EnvironmentUtils.isDocker()) {
      info.add(DOCKER_KEY);
    }
    if (EnvironmentUtils.isKubernetes()) {
      info.add(KUBERNETES_KEY);
    }
    if (EnvironmentUtils.isGoogleComputeEngine()) {
      info.add(GCE_KEY);
    } else {
      addEC2Info(info);
    }
  }

  /**
   * Get the feature's information.
   *
   * @param info the list to add info to
   */
  @VisibleForTesting
  public static void addUserAgentFeatures(List<String> info) {
    addIfTrue(FeatureUtils.isEmbeddedJournal(), info, EMBEDDED_KEY);
    addIfTrue(FeatureUtils.isRocks(), info, ROCKS_KEY);
    addIfTrue(FeatureUtils.isZookeeperEnabled(), info, ZOOKEEPER_KEY);
    addIfTrue(FeatureUtils.isBackupDelegationEnabled(), info, BACKUP_DELEGATION_KEY);
    addIfTrue(FeatureUtils.isDailyBackupEnabled(), info, DAILY_BACKUP_KEY);
    addIfTrue(!FeatureUtils.isPersistenceBlacklistEmpty(), info, PERSIST_BLACK_LIST_KEY);
    addIfTrue(FeatureUtils.isUnsafeDirectPersistEnabled(), info, UNSAFE_PERSIST_KEY);
    addIfTrue(FeatureUtils.isMasterAuditLoggingEnabled(), info, MASTER_AUDIT_LOG_KEY);
    addIfTrue(FeatureUtils.isPageStoreEnabled(), info, PAGE_STORE_KEY);
  }

  /**
   * Add feature name if condition is true.
   *
   * @param valid true, if condition is valid
   * @param features feature list
   * @param featureName feature name
   */
  private static void addIfTrue(boolean valid, List<String> features, String featureName) {
    if (valid) {
      features.add(featureName);
    }
  }

  /**
   * Adds the information of EC2 environment to given list.
   *
   * @param info the list to add info to
   */
  private static void addEC2Info(List<String> info) {
    boolean isEC2 = false;
    String productCode = EnvironmentUtils.getEC2ProductCode();
    if (!productCode.isEmpty()) {
      info.add(PRODUCT_CODE_KEY + productCode);
      isEC2 = true;
    }

    String userData = "";
    try {
      userData = EC2MetadataUtils.getUserData();
    } catch (Throwable t) {
      // Exceptions are expected if instance is not EC2 instance
      // or get metadata operation is not allowed
    }
    if (userData != null && !userData.isEmpty()) {
      isEC2 = true;
      if (EnvironmentUtils.isCFT(userData)) {
        info.add(CFT_KEY);
      } else if (EnvironmentUtils.isEMR(userData)) {
        info.add(EMR_KEY);
      }
    } else if (!isEC2 && EnvironmentUtils.isEC2()) {
      isEC2 = true;
    }

    if (isEC2) {
      info.add(EC2_KEY);
    }
  }

  private UpdateCheck() {} // prevent instantiation
}
