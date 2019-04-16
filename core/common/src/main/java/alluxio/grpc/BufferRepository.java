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

import java.io.Closeable;

/**
 * A repository of buffers.
 *
 * @param <TMesg> type of the message
 * @param <TBuf> type of the buffer
 */
public interface BufferRepository<TMesg, TBuf> extends Closeable {
  /**
   * Stores a buffer in the repository.
   *
   * @param buffer the buffer to store
   * @param message the associated message
   */
  void offerBuffer(TBuf buffer, TMesg message);

  /**
   * Retrieves and removes a buffer from the store.
   *
   * @param message the message that associated with the buffer
   * @return the buffer, or null if the buffer is not found
   */
  TBuf pollBuffer(TMesg message);

  /**
   * Closes the repository and all its buffers.
   */
  void close();
}
