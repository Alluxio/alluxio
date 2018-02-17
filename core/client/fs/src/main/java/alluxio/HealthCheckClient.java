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

package alluxio;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Client for determining a service is serving RPCs.
 */
@ThreadSafe
public interface HealthCheckClient {
  /**
   * @return true if the service is serving RPC, otherwise false. The implementation should
   *         perform retries if appropriate
   */
  boolean isServing();
}
