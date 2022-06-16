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

package alluxio.exception;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Exception used when the service is not provided by the server.
 *
 */
@ThreadSafe
public final class ServiceNotFoundException extends AlluxioException {
  private static final long serialVersionUID = 6180404970709548L;

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause
   */
  public ServiceNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
