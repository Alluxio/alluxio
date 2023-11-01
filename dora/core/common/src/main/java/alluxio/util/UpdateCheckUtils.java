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

package alluxio.util;

import alluxio.ProjectConstants;
import alluxio.exception.runtime.FailedPreconditionRuntimeException;

import com.amazonaws.util.EC2MetadataUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
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
public final class UpdateCheckUtils {
  public static final String USER_AGENT_SEPARATOR = ";";

  static final String PRODUCT_CODE_FORMAT = "ProductCode:%s";
  static final String OS_FORMAT = "OS:%s";

  // Launch environment
  static final String CFT_KEY = "cft";
  static final String DOCKER_KEY = "docker";
  static final String EC2_KEY = "ec2";
  static final String EMR_KEY = "emr";
  static final String GCE_KEY = "gce";
  static final String KUBERNETES_KEY = "kubernetes";

  /**
   * @param id the id of the current Alluxio identity (e.g. cluster id, instance id)
   * @param processType process type
   * @param additionalInfo additional information to send
   * @return the latest Alluxio version string
   */
  public static String getLatestVersion(String id, CommonUtils.ProcessType processType,
      List<String> additionalInfo) throws IOException {
    Preconditions.checkState(id != null && !id.isEmpty(), "id should not be null or empty");
    Preconditions.checkNotNull(additionalInfo);
    // Create the GET request.
    Joiner joiner = Joiner.on("/");
    String path = joiner.join("v1", "version");
    String url = new URL(new URL(ProjectConstants.UPDATE_CHECK_HOST), path).toString();

    HttpGet post = new HttpGet(url);
    post.setHeader("User-Agent", getUserAgentString(id, processType, additionalInfo));
    post.setHeader("Authorization", "Basic " + ProjectConstants.UPDATE_CHECK_MAGIC_NUMBER);

    // Fire off the version check request.
    HttpClient client = HttpClientBuilder.create()
        .setDefaultRequestConfig(
            RequestConfig.custom()
                .setConnectionRequestTimeout(3000)
                .setConnectTimeout(3000)
                .setSocketTimeout(3000)
                .build())
        .build();
    HttpResponse response = client.execute(post);

    // Check the response code.
    int responseCode = response.getStatusLine().getStatusCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      throw new FailedPreconditionRuntimeException(
          "Update check request failed with code: " + responseCode);
    }

    return EntityUtils.toString(response.getEntity(), "UTF-8");
  }

  /**
   * @param id the id of the current Alluxio identity (e.g. cluster id, instance id)
   * @param processType process type
   * @param additionalInfo additional information to add to result string
   * @return a string representation of the user's environment in the format
   *         "Alluxio/{ALLUXIO_VERSION} (valueA; valueB)"
   */
  @VisibleForTesting
  public static String getUserAgentString(String id, CommonUtils.ProcessType processType,
      List<String> additionalInfo) {
    List<String> info = new ArrayList<>();
    info.add(id);
    addUserAgentEnvironments(info);
    info.addAll(additionalInfo);
    info.add(String.format("processType:%s", processType.toString()));
    return String.format("Alluxio/%s (%s)", ProjectConstants.VERSION,
        Joiner.on(USER_AGENT_SEPARATOR + " ").skipNulls().join(info));
  }

  /**
   * Adds the information of user environment to given list.
   *
   * @param info the list to add info to
   */
  @VisibleForTesting
  public static void addUserAgentEnvironments(List<String> info) {
    info.add(String.format(OS_FORMAT, OSUtils.OS_NAME));
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
   * Adds the information of EC2 environment to given list.
   *
   * @param info the list to add info to
   */
  private static void addEC2Info(List<String> info) {
    boolean isEC2 = false;
    String productCode = EnvironmentUtils.getEC2ProductCode();
    if (!productCode.isEmpty()) {
      info.add(String.format(PRODUCT_CODE_FORMAT, productCode));
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

  private UpdateCheckUtils() {} // prevent instantiation
}
