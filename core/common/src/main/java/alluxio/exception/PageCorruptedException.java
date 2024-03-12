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
 * An exception that should be thrown when the data of a page has been corrupted in store.
 */
public class PageCorruptedException extends RuntimeException {

  /**
   * Construct PageCorruptedException with the specified message.
   * @param message
   */
  public PageCorruptedException(String message) {
    super(message);
  }

  /**
   * Construct PageCorruptedException with the specified message and cause.
   * @param message
   * @param cause
   */
  public PageCorruptedException(String message, Throwable cause) {
    super(message, cause);
  }
}
