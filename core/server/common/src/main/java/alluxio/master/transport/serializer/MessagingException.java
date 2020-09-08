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

package alluxio.master.transport.serializer;

/**
 * Messaging exception.
 */
public class MessagingException extends RuntimeException {

  /**
   * Constructs a new {@link MessagingException}.
   */
  public MessagingException() {
  }

  /**
   * Constructs a new {@link MessagingException} with message.
   *
   * @param message the exception message
   */
  public MessagingException(String message) {
    super(message);
  }

  /**
   * Constructs a new {@link MessagingException} with message and cause.
   *
   * @param message the exception message
   * @param cause cause of the exception
   */
  public MessagingException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new {@link MessagingException} with cause.
   *
   * @param cause cause of the exception
   */
  public MessagingException(Throwable cause) {
    super(cause);
  }
}
