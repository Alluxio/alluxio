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

import alluxio.proto.dataserver.Protocol;

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The block write request internal representation.
 */
@ThreadSafe
public final class BlockWriteRequest extends WriteRequest {
  /** Which tier this block writes to. */
  private final int mTier;
  private final String mMediumType;

  /**
   * @param request block request in proto
   */
  BlockWriteRequest(alluxio.grpc.WriteRequest request) {
    super(request);
    mTier = request.getCommand().getTier();
    mMediumType = request.getCommand().getMediumType();
  }

  /**
   * @return the tier this block is writing to
   */
  public int getTier() {
    return mTier;
  }

  /**
   * @return the medium type this block is writing to
   */
  public String getMediumType() {
    return mMediumType;
  }

  @Override
  protected MoreObjects.ToStringHelper toStringHelper() {
    return super.toStringHelper().add("tier", mTier)
        .add("mediumType", mMediumType);
  }
}
