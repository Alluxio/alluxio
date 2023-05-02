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

/**
 * A struct that carries a message with a data buffer.
 *
 * @param <T> the type of the message
 * @param <R> the type of the data buffer
 */
public class DataMessage<T, R> {
  private final T mMessage;
  private final R mBuffer;

  /**
   * @param message the message
   * @param buffer the data buffer
   */
  public DataMessage(T message, R buffer) {
    mMessage = message;
    mBuffer = buffer;
  }

  /**
   * @return the message
   */
  public T getMessage() {
    return mMessage;
  }

  /**
   * @return the data buffer
   */
  public R getBuffer() {
    return mBuffer;
  }
}
