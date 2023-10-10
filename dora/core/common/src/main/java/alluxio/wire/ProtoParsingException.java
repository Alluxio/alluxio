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

package alluxio.wire;

import alluxio.exception.AlluxioException;

/**
 * Thrown when parsing fails.
 */
public class ProtoParsingException extends AlluxioException {
  protected ProtoParsingException(Throwable cause) {
    super(cause);
  }

  /**
   * Constructs a new exception with detailed message.
   * @param message
   */
  public ProtoParsingException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with detailed message and cause.
   * @param message
   * @param cause
   */
  public ProtoParsingException(String message, Throwable cause) {
    super(message, cause);
  }
}
