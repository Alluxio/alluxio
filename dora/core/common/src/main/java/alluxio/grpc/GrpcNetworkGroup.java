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

package alluxio.grpc;

/**
 * Used to define connection level multiplexing groups.
 */
public enum GrpcNetworkGroup {
  /**
   * Networking group for RPC traffic.
   */
  RPC,
  /**
   * Networking group for Streaming traffic.
   */
  STREAMING,
  /**
   * Networking group for secret exchange.
   */
  SECRET
  ;

  /**
   * @return the code used to refer to this group in property key templates
   */
  public String getPropertyCode() {
    switch (this) {
      case RPC:
        return "rpc";
      case STREAMING:
        return "streaming";
      default:
        throw new IllegalArgumentException(
            String.format("Unrecognized network group: %s", this.toString()));
    }
  }
}
