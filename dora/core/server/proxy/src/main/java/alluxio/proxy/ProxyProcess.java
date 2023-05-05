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

package alluxio.proxy;

import alluxio.Process;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A proxy in the Alluxio system.
 */
public interface ProxyProcess extends Process {
  /**
   * Factory for creating {@link ProxyProcess}.
   */
  @ThreadSafe
  final class Factory {
    /**
     * @return a new instance of {@link ProxyProcess}
     */
    public static ProxyProcess create() {
      return new AlluxioProxyProcess();
    }

    private Factory() {} // prevent instantiation
  }

  /**
   * @return the start time of the worker in milliseconds
   */
  long getStartTimeMs();

  /**
   * @return the uptime of the worker in milliseconds
   */
  long getUptimeMs();

  /**
   * @return the worker web service port (used by unit test only)
   */
  int getWebLocalPort();
}
