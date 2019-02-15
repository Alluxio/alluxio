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

import alluxio.exception.status.AlluxioStatusException;

import io.grpc.StatusException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for conversion between alluxio and grpc status exceptions.
 */
@ThreadSafe
public final class GrpcExceptionUtils {
  /**
   * Converts a throwable to a gRPC exception.
   *
   * @param e throwable
   * @return gRPC exception
   */
  public static StatusException fromThrowable(Throwable e) {
    return AlluxioStatusException.fromThrowable(e).toGrpcStatusException();
  }
}
