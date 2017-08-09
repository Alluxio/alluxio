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

package alluxio.worker.netty;

import alluxio.util.IdUtils;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents a read request received from netty channel.
 */
@ThreadSafe
class ReadRequest {
  private final long mId;
  private final long mStart;
  private final long mEnd;
  private final long mPacketSize;
  private final long mSessionId;

  ReadRequest(long id, long start, long end, long packetSize) {
    mId = id;
    mStart = start;
    mEnd = end;
    mPacketSize = packetSize;
    mSessionId = IdUtils.createSessionId();
  }

  /**
   * @return session Id
   */
  public long getSessionId() {
    return mSessionId;
  }

  /**
   * @return block id of the read request
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the start offset in bytes of this read request
   */
  public long getStart() {
    return mStart;
  }

  /**
   * @return the end offset in bytes of this read request
   */
  public long getEnd() {
    return mEnd;
  }

  /**
   * @return the packet size in bytes of this read request
   */
  public long getPacketSize() {
    return mPacketSize;
  }
}
