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

import alluxio.annotation.SuppressFBWarnings;
import alluxio.util.io.FileUtils;

import com.amazonaws.util.EC2MetadataUtils;
import com.google.common.io.CharStreams;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utilities to detect environment Alluxio is running in.
 */
@ThreadSafe
public final class EnvironmentUtils {
  private static final Logger LOG = LoggerFactory.getLogger(EnvironmentUtils.class);

  /**
   * Utility to detect the docker deployment environment.
   *
   * @return true, if running on docker
   */
  @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
  public static boolean isDocker() {
    return FileUtils.exists("/.dockerenv");
  }

  /**
   * Utility to detect the k8s deployment environment.
   *
   * @return true, if running on k8s
   */
  public static boolean isKubernetes() {
    return System.getenv("KUBERNETES_SERVICE_HOST") != null;
  }

  /**
   * Utility to detect the EC2 deployment environment.
   *
   * @return true, if running on EC2
   */
  public static boolean isEC2() {
    return isEC2WithUUID() || isEC2WithProductUUID() || isEC2WithInstanceIdentity();
  }

  /**
   * Utility to detect the Google compute engine deployment environment.
   *
   * @return true, if running on gce
   */
  public static boolean isGoogleComputeEngine() {
    return isGCEWithMetadata() || isGCEWithBiosVendor();
  }

  /**
   * Gets the EC2 product code if any.
   *
   * @return the first product code if any, an empty string otherwise
   */
  public static String getEC2ProductCode() {
    try {
      List<String> productCodes = EC2MetadataUtils.getProductCodes();
      if (productCodes.size() < 1) {
        return "";
      }
      return productCodes.get(0);
    } catch (Throwable t) {
      // Exceptions are expected if this instance is not EC2 instance
      // or this EC2 does not have product code
      return "";
    }
  }

  /**
   * Checks whether the given user data belongs to an instance launched
   * through CFT.
   *
   * @param userData the ec2 instance user data
   * @return true if this instance is launched from CFT, false otherwise
   */
  public static boolean isCFT(String userData) {
    return !userData.isEmpty() && userData.contains("cft_configure");
  }

  /**
   * Checks whether the given user data belongs to an instance launched
   * through EMR.
   *
   * @param userData the ec2 instance user data
   * @return true if this instance is launched from EMR, false otherwise
   */
  public static boolean isEMR(String userData) {
    return !userData.isEmpty() && userData.contains("emr-apps")
        && userData.contains("emr-platform");
  }

  /**
   * Check if this instance is an EC2 instance with UUID file.
   * This method does not work on new m5 or c5 ec2 instances.
   *
   * @return true if jvm runs in EC2 instance with UUID file, false otherwise
   */
  @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
  private static boolean isEC2WithUUID() {
    try {
      return ec2UUIDFileExistsWithID("/sys/hypervisor/uuid", "ec2");
    } catch (Throwable t) {
      // Exceptions are expected if this instance is not EC2 instance
      // or this check is not valid
      return false;
    }
  }

  /**
   * Check if this instance is an EC2 instance with product UUID file
   * This method does not work on new m5 or c5 instances.
   *
   * @return true if jvm runs in EC2 instance with product UUID file, false otherwise
   */
  @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
  private static boolean isEC2WithProductUUID() {
    try {
      return ec2UUIDFileExistsWithID("/sys/devices/virtual/dmi/id/product_uuid", "EC2");
    } catch (Throwable t) {
      // Exceptions are expected if this instance is not EC2 instance
      // or this check is not valid
      return false;
    }
  }

  /**
   * Check if this instance is an EC2 instance with instance identity.
   * Note that other cloud providers make this instance metadata URL available
   * and may thus cause false positive.
   *
   * @return true if jvm runs in EC2 instance with instance identity, false otherwise
   */
  private static boolean isEC2WithInstanceIdentity() {
    try {
      EC2MetadataUtils.getInstanceInfo();
      return true;
    } catch (Throwable t) {
      // Exceptions are expected if this instance is not EC2 instance
      // or this check is not valid
      return false;
    }
  }

  /**
   * @return true, if running on GCE with google metadata available
   */
  private static boolean isGCEWithMetadata() {
    try {
      String url = "http://metadata.google.internal/computeMetadata/v1/instance/zone";
      HttpGet post = new HttpGet(url);
      post.setHeader("Metadata-Flavor", "Google");
      HttpClient client = HttpClientBuilder.create()
          .setDefaultRequestConfig(
              RequestConfig.custom()
                  .setConnectionRequestTimeout(1000)
                  .setConnectTimeout(1000)
                  .setSocketTimeout(1000)
                  .build())
          .build();
      HttpResponse response = client.execute(post);

      int responseCode = response.getStatusLine().getStatusCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        return false;
      }
      String zone = EntityUtils.toString(response.getEntity(), "UTF-8");
      return !zone.isEmpty();
    } catch (Throwable t) {
      // Exceptions are expected if this instance is not GCE instance
      // or this GCE does not allow fetching metadata
      return false;
    }
  }

  /**
   * @return true, if running on GCE with Google bios vendor
   */
  private static boolean isGCEWithBiosVendor() {
    try {
      Process process = Runtime.getRuntime().exec("sudo dmidecode -s bios-vendor");
      try (Reader reader = new InputStreamReader(process.getInputStream())) {
        String output = CharStreams.toString(reader);
        return output.contains("Google");
      }
    } catch (Throwable t) {
      // Exceptions are expected if this instance is not
      // running on Google bios vendor
      return false;
    }
  }

  /**
   * Checks if the target ec2 UUID file exists and starts with the identifier string.
   *
   * @param filePath the file to check
   * @param identifier the string identifier to check
   * @return true if file exists and starts with the identifier
   */
  private static boolean ec2UUIDFileExistsWithID(String filePath,
      String identifier) throws IOException {
    if (FileUtils.exists(filePath)) {
      int length = identifier.length();
      byte[] array = new byte[length];
      try (InputStream in = new FileInputStream(filePath)) {
        int offset = 0;
        while (offset < length) {
          int read = in.read(array, offset, length - offset);
          if (read == -1) {
            break;
          }
          offset += read;
        }
        if (offset != length) {
          return false;
        }
        String content = new String(array);
        if (content.equals(identifier)) {
          return true;
        }
      }
    }
    return false;
  }

  private EnvironmentUtils() {} // prevent instantiation
}
