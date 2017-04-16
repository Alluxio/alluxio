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

package alluxio.network.protocol;

import alluxio.proto.dataserver.Protocol;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Util functions for {@link alluxio.proto.dataserver.Protocol.Status}.
 */
@ThreadSafe
public final class Status {
  /**
   * @param status the status
   * @return true if the status is in OK state
   */
  public static boolean isOk(Protocol.Status status) {
    return status.getCode() == Protocol.Status.Code.OK;
  }

  private Status() {} // prevent instantiation
}
