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

package alluxio.cli.hms;

import alluxio.cli.ValidationTaskResult;
import alluxio.cli.ValidationUtils;
import alluxio.collections.Pair;
import alluxio.util.CommonUtils;

import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A Task which given an input string, checks that the input is a valid connection string for to
 * the Hive Metastore.
 */
public class UriCheckTask extends MetastoreValidationTask<Void, String> {

  private final String mUris;
  private final int mTimeoutMs;

  /**
   * Creates a new {@link UriCheckTask}.
   *
   * @param inputUri the input URI(s) to check
   * @param timeoutMs the duration to attempt to connect to the URI before failing
   */
  public UriCheckTask(String inputUri, int timeoutMs) {
    super(null);
    mUris = inputUri;
    mTimeoutMs = timeoutMs;
  }

  @Override
  Pair<ValidationTaskResult, String> getValidationWithResult() {
    if (mUris == null || mUris.isEmpty()) {
      String errorMessage = "Hive metastore uris must be provided";
      return new Pair<>(new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
          errorMessage, "Please provide the hive metastore uris"), null);
    }
    List<String> uris;
    if (mUris.contains(",")) {
      uris = Arrays.asList(mUris.split(","));
    } else {
      uris = Collections.singletonList(mUris);
    }
    for (String rawUri : uris) {
      URI uri;
      try {
        uri = new URI(rawUri);
      } catch (Throwable t) {
        return new Pair<>(new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
            ValidationUtils.getErrorInfo(t),
            "Please make sure the given hive metastore uri(s) is valid"), null);
      }

      if (uri.getHost() == null || uri.getPort() == -1 || !uri.getScheme().equals("thrift")) {
        String errorMessage = "Invalid hive metastore uris";
        return new Pair<>(
            new ValidationTaskResult(ValidationUtils.State.FAILED, getName(), errorMessage,
                "Please make sure the given hive metastore uri(s) is valid"), null);
      }

      try {
        InetAddress.getByName(uri.getHost());
      } catch (Throwable t) {
        return new Pair<>(new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
            ValidationUtils.getErrorInfo(t),
            "Please make sure the hostname in given hive metastore uri(s) is resolvable"), null);
      }

      if (!CommonUtils.isAddressReachable(uri.getHost(), uri.getPort(), mTimeoutMs)) {
        String errorMessage = "Hive metastore uris are unreachable";
        return new Pair<>(new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
            errorMessage, "Please make sure the given hive metastore uris are reachable. Check"
            + " that both the host and port are correct."), null);
      }
    }

    return new Pair<>(new ValidationTaskResult(ValidationUtils.State.OK, getName(),
        "metastore syntax check passed", ""), mUris);
  }

  @Override
  public String getName() {
    return "HmsUrisCheckTask";
  }
}
