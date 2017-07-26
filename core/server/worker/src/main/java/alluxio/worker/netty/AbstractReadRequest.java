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

import java.io.Closeable;

/**
 * Represents the read request received from netty channel.
 */
abstract class AbstractReadRequest implements Closeable {
  final long mId;
  final long mStart;
  final long mEnd;
  final long mPacketSize;
  final long mSessionId;

  AbstractReadRequest(long id, long start, long end, long packetSize) {
    mId = id;
    mStart = start;
    mEnd = end;
    mPacketSize = packetSize;
    mSessionId = IdUtils.createSessionId();
  }
}
