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

package alluxio.master.meta;

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
  private static final String PRODUCT_CODE_KEY = "ProductCode:";

  /**
   * @param clusterID the cluster ID
   * @param connectionRequestTimeout the connection request timeout for the HTTP request in ms
   * @param connectTimeout the connect timeout for the HTTP request in ms
   * @param socketTimeout the socket timeout for the HTTP request in ms
   * @return the latest Alluxio version string
   */
  public static String getLatestVersion(String clusterID, long connectionRequestTimeout,
      long connectTimeout, long socketTimeout) throws IOException {
    // Create the GET request.
    Joiner joiner = Joiner.on("/");
    String path = joiner.join("v0", "version");
    String url = new URL(new URL(ProjectConstants.UPDATE_CHECK_HOST), path).toString();

    HttpGet post = new HttpGet(url);
    post.setHeader("User-Agent", getUserAgentString(clusterID));
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
   * @return a string representation of the user's environment in the format "key1:value1, key2:
   *         value2".
   */
  @VisibleForTesting
  public static String getUserAgentString(String clusterID) throws IOException {
    Joiner joiner = Joiner.on("; ").skipNulls();
    String sysInfo = joiner.join(getUserAgentEnvironmentString(clusterID), getFeatureString());
    return String.format("Alluxio/%s (%s)", ProjectConstants.VERSION, sysInfo);
  }

  /**
   * @param clusterID the cluster ID
   * @return a string representation of the user's environment in the format "key1:value1, key2:
   *         value2".
   */
  @VisibleForTesting
  public static String getUserAgentEnvironmentString(String clusterID) throws IOException {
    Joiner joiner = Joiner.on("; ").skipNulls();
    boolean isGCE = EnvironmentUtils.isGoogleComputeEngine();
    String sysInfo = joiner.join(
        clusterID,
        EnvironmentUtils.isDocker() ? "docker" : null,
        EnvironmentUtils.isKubernetes() ? "kubernetes" : null,
        isGCE ? "gce" : null
    );
    if (!isGCE) {
      List<String> ec2Info = getEC2Info();
      if (ec2Info.size() != 0) {
        sysInfo = joiner.join(sysInfo, joiner.join(ec2Info));
      }
    }
    return sysInfo;
  }

  /**
   * Get the features information.
   *
   * @return a list of string represent feature names
   */
  @VisibleForTesting
  public static String getFeatureString() {
    Joiner joiner = Joiner.on("; ").skipNulls();
    List<String> featureInfo = new ArrayList<>();
    if (FeatureUtils.isEmbeddedJournal()) {
      featureInfo.add("embedded");
    }
    if (FeatureUtils.isRocks()) {
      featureInfo.add("rocks");
    }
    if (FeatureUtils.isZookeeperEnable()) {
      featureInfo.add("zk");
    }
    if (FeatureUtils.isBackupDelegationEnable()) {
      featureInfo.add("backupDelegation");
    }
    if (FeatureUtils.isDailyBackupEnable()) {
      featureInfo.add("dailyBackup");
    }
    if (!FeatureUtils.isPersistenceBlacklistEmpty()) {
      featureInfo.add("persistBlackList");
    }
    if (FeatureUtils.isUnsafeDirectPersistEnable()) {
      featureInfo.add("unsafePersist");
    }
    if (FeatureUtils.isMasterAuditLoggingEnable()) {
      featureInfo.add("masterAuditLog");
    }
    if (featureInfo.size() == 0) {
      return null;
    }
    return joiner.join(featureInfo);
  }

  /**
   * Gets the EC2 system information.
   *
   * @return a list of string representation of the user's EC2 environment
   */
  private static List<String> getEC2Info() {
    List<String> ec2Info = new ArrayList<>();
    boolean isEC2 = false;
    String productCode = EnvironmentUtils.getEC2ProductCode();
    if (!productCode.isEmpty()) {
      ec2Info.add(PRODUCT_CODE_KEY + productCode);
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
        ec2Info.add("cft");
      } else if (EnvironmentUtils.isEMR(userData)) {
        ec2Info.add("emr");
      }
    } else if (!isEC2 && EnvironmentUtils.isEC2()) {
      isEC2 = true;
    }

    if (isEC2) {
      ec2Info.add("ec2");
    }
    return ec2Info;
  }

  private UpdateCheck() {} // prevent instantiation
}
