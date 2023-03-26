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

package alluxio.client.block.stream;

import alluxio.client.file.cache.store.PageReadTargetBuffer;
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.network.protocol.databuffer.DataBuffer;

import java.io.Closeable;
import java.io.IOException;

/**
 * The interface to read data chunks.
 */
public interface DataReader extends Closeable {

  /**
   * Reads a chunk. The caller needs to release the chunk.
   *
   * @return the data buffer or null if EOF is reached
   */
  DataBuffer readChunk() throws IOException;

  /**
   * Reads the data into given buffer.
   *
   * @param buffer the target buffer
   * @return bytes read, or -1 if EOF
   */
  default int read(PageReadTargetBuffer buffer) {
    throw new UnimplementedRuntimeException("read to buffer is not implemented");
  }

  /**
   * @return the current stream position
   */
  long pos();

  /**
   * The factory interface to create {@link DataReader}s.
   */
  interface Factory extends Closeable {
    /**
     * Creates an instance of {@link DataReader}.
     *
     * @param offset the stream offset
     * @param len the length of the stream
     * @return the created object
     */
    DataReader create(long offset, long len) throws IOException;
  }
}
