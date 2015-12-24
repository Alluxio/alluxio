/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client;

import com.google.common.base.Objects;

import tachyon.annotation.PublicApi;

/**
 * The network of a worker.
 */
@PublicApi
public final class WorkerNetAddress {
  private final String mHost;
  private final int mRpcPort;
  private final int mDataPort;
  private final int mWebPort;

  /**
   * Constructs the worker net address.
   */
  public WorkerNetAddress(String host, int rpcPort, int dataPort, int webPort) {
    mHost = host;
    mRpcPort = rpcPort;
    mDataPort = dataPort;
    mWebPort = webPort;
  }

  /**
   * @return the host of the worker
   */
  public String getHost() {
    return mHost;
  }

  /**
   * @return the RPC port
   */
  public int getRpcPort() {
    return mRpcPort;
  }

  /**
   * @return the data port
   */
  public int getDataPort() {
    return mDataPort;
  }

  /**
   * @return the web port
   */
  public int getWebPort() {
    return mWebPort;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WorkerNetAddress)) {
      return false;
    }
    WorkerNetAddress that = (WorkerNetAddress) o;
    return mHost.equals(that.mHost) && mRpcPort == that.mRpcPort && mDataPort == that.mDataPort
        && mWebPort == that.mWebPort;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mHost, mDataPort, mRpcPort, mWebPort);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("host", mHost).add("rpcPort", mRpcPort)
        .add("dataPort", mDataPort).add("webPort", mWebPort).toString();
  }
}
