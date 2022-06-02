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
 * The exception thrown when an AlluxioSecondaryMaster start with EMBEDDED JournalType.
 */
public class UnsupportedJournalTypeException extends AlluxioException {
  private static final long serialVersionUID = 964881830415517869L;

  /**
   * Constructs a new exception with the cause.
   *
   * @param cause the cause
   */
  protected UnsupportedJournalTypeException(Throwable cause) {
    super(cause);
  }

  /**
   * Constructs a new exception with the specified exception message.
   *
   * @param message the exception message
   */
  public UnsupportedJournalTypeException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified exception message.
   *
   * @param message the exception message
   */
  public UnsupportedJournalTypeException(ExceptionMessage message) {
      this(message.getMessage());
  }
}
