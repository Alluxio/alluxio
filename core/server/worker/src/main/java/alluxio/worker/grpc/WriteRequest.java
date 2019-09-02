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

package alluxio.worker.grpc;

import alluxio.util.IdUtils;

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents a write request received from gRPC channel.
 */
@ThreadSafe
public class WriteRequest {
  /** This ID can either be block ID or temp UFS file ID. */
  private final long mId;

  /** Whether to pin block on create. */
  private boolean mPinOnCreate;

  /** The session id associated with all temporary resources of this request. */
  private final long mSessionId;

  WriteRequest(alluxio.grpc.WriteRequest request) {
    mId = request.getCommand().getId();
    mPinOnCreate = request.getCommand().getPinOnCreate();
    mSessionId = IdUtils.createSessionId();
  }

  /**
   * @return the block ID or the temp UFS file ID
   */
  public long getId() {
    return mId;
  }

  /**
   * @return whether to pin block on create
   */
  public boolean getPinOnCreate() {
    return mPinOnCreate;
  }

  /**
   * @return the session ID
   */
  public long getSessionId() {
    return mSessionId;
  }

  /**
   * @return a {@link MoreObjects.ToStringHelper}, inheriting classes should call super
   */
  protected MoreObjects.ToStringHelper toStringHelper() {
    return MoreObjects.toStringHelper(this)
        .add("id", mId)
        .add("sessionId", mSessionId);
  }

  @Override
  public final String toString() {
    return toStringHelper().toString();
  }
}
