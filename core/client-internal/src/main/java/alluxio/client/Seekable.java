/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client;

import java.io.IOException;

/**
 * This interface should be implemented by all Alluxio streams which support moving the read
 * position to a specific byte offset.
 */
// TODO(calvin): Evaluate if this should be ByteSeekable.
public interface Seekable {
  /**
   * Moves the starting read position of the stream to the specified position which is relative to
   * the start of the stream. Seeking to a position before the current read position is supported.
   *
   * @param pos the position to seek to, it must be between 0 and the end of the stream - 1
   * @throws IOException if the seek fails due to an error accessing the stream at the position
   */
  void seek(long pos) throws IOException;
}
