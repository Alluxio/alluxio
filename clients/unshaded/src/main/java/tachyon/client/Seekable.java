/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client;

import java.io.IOException;

/**
 * This interface should be implemented by all Tachyon streams which support moving the read
 * position to a specific byte offset.
 */
// TODO(calvin): Evaluate if this should be ByteSeekable.
public interface Seekable {
  /**
   * Moves the starting read position of the stream to the specified position which is relative to
   * the start of the stream. Seeking to a position before the current read position is supported.
   *
   * @param pos The position to seek to, it must be between 0 and the end of the stream - 1.
   * @throws IOException if the seek fails due to an error accessing the stream at the position
   */
  void seek(long pos) throws IOException;
}
