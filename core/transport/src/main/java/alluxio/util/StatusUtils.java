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

package alluxio.util;

import alluxio.proto.status.Status;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities to convert to from Status exceptions.
 */
@ThreadSafe
public final class StatusUtils {
  private static final Logger LOG = LoggerFactory.getLogger(StatusUtils.class);

  /**
   * Convert from proto to {@link alluxio.exception.status.Status}.
   *
   * @param status
   * @return
   */
  public static alluxio.exception.status.Status fromProto(Status.PStatus status) {
    return null;
  }

  /**
   * Convert from {@link alluxio.exception.status.Status} to proto.
   *
   * @param status
   * @return
   */
  public static Status.PStatus toProto(alluxio.exception.status.Status status) {
    return null;
  }

  private StatusUtils() {} // prevent instantiation
}
