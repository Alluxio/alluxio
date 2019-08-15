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

import com.google.common.base.Joiner;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Check for updates.
 */
@ThreadSafe
public final class UpdateCheck {
  /**
   * @return the latest Alluxio version string
   */
  public static String getLatestVersion() throws IOException {
    // Create the GET request.
    Joiner joiner = Joiner.on("/");
    String path = joiner.join("v0", "version");
    String url = new URL(new URL(ProjectConstants.UPDATE_CHECK_HOST), path).toString();

    HttpGet post = new HttpGet(url);
    post.setHeader("User-Agent", getEnvString());
    post.setHeader("Authorization", "Basic " + ProjectConstants.UPDATE_CHECK_AUTH_STRING);

    // Fire off the upload request.
    HttpClient client = HttpClientBuilder.create().build();
    HttpResponse response = client.execute(post);

    // Check the response code.
    int responseCode = response.getStatusLine().getStatusCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      throw new IOException("Update check request failed with code: " + responseCode);
    }

    return EntityUtils.toString(response.getEntity(), "UTF-8");
  }

  /**
   * @return a string representation of the user's environment in the format "key1:value1, key2:
   *         value2".
   */
  private static String getEnvString() throws IOException {
    Joiner joiner = Joiner.on(",");
    return joiner.join(
      "AlluxioVersion: " + ProjectConstants.VERSION,
        "IsDocker: " + EnvironmentUtils.isDocker(),
        "IsKubernetes: " + EnvironmentUtils.isKubernetes()
    );
  }

  private UpdateCheck() {} // prevent instantiation
}
