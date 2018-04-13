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

package alluxio.worker;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.io.PathUtils;

/**
 * This class provides utility methods for workers.
 */
public final class WorkerUtils {
  private WorkerUtils() {}  // prevent instantiation

  /**
   * Get the domain socket address for the given worker id.
   *
   * @param workerId the worker id
   * @return the domain socket file system path
   */
  public static String getDomainSocketAddress(final Long workerId) {
    String domainSocketPath =
        Configuration.get(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS);
    if (Configuration.getBoolean(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_WORKER_ID)) {
      domainSocketPath = PathUtils.concatPath(domainSocketPath, workerId);
    }
    return domainSocketPath;
  }
}
