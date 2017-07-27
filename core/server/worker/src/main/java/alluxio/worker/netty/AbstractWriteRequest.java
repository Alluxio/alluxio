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

import com.codahale.metrics.Counter;
import io.netty.channel.Channel;

import java.io.IOException;

import javax.annotation.Nullable;

/**
 * Represents the write request received from netty channel.
 */
abstract class AbstractWriteRequest {
  /** This ID can either be block ID or temp UFS file ID. */
  private final long mId;

  /** The session id associated with all temporary resources of this request. */
  private final long mSessionId;
  private Counter mCounter;

  AbstractWriteRequest(long id) {
    mId = id;
    mSessionId = IdUtils.createSessionId();
  }

  /**
   * @return the block ID or the temp UFS file ID
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the session ID
   */
  public long getSessionId() {
    return mSessionId;
  }

  /**
   * @return the counter
   */
  @Nullable
  public Counter getCounter() {
    return mCounter;
  }

  /**
   * Sets the counter.
   *
   * @param counter counter to set
   */
  public void setCounter(Counter counter) {
    mCounter = counter;
  }

  /**
   * Closes the request.
   *
   * @param channel the channel
   */
  abstract void close(Channel channel) throws IOException;

  /**
   * Cancels the request.
   */
  abstract void cancel() throws IOException;

  /**
   * Cleans up the state.
   */
  abstract void cleanup() throws IOException;
}
