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

package alluxio.exception.status;

import alluxio.exception.AlluxioRuntimeException;

import io.grpc.Status;

/**
 * Exception indicating that some resource has been exhausted, perhaps a per-user quota, or perhaps
 * the task queue.
 */
public class ResourceExhaustedRuntimeException extends AlluxioRuntimeException {
  private static final Status STATUS = Status.RESOURCE_EXHAUSTED;
  private static final boolean RETRYABLE = true;

  /**
   * Constructor.
   * @param message error message
   */
  public ResourceExhaustedRuntimeException(String message) {
    super(STATUS, message, RETRYABLE);
  }
}
