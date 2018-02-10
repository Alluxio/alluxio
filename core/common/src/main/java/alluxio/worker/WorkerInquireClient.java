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

import alluxio.exception.status.UnavailableException;

import javax.annotation.concurrent.ThreadSafe;

import java.net.InetSocketAddress;

/**
 * Client for determining a worker is serving RPCs.
 */
@ThreadSafe
public interface WorkerInquireClient {
  /**
   * @return the rpc address of the worker. The implementation should perform retries if
   *         appropriate
   * @throws UnavailableException if the rpc address cannot be determined
   */
  InetSocketAddress getRpcAddress() throws UnavailableException;
}
