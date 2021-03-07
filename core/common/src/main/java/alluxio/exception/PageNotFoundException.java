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

/**
 * An exception that should be thrown when a page is not found in store.
 */
public class PageNotFoundException extends AlluxioException {
  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   */
  public PageNotFoundException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause
   */
  public PageNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
