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

package alluxio.job.replicate;

import alluxio.job.JobConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Configuration of a job evicting a block.
 */
@ThreadSafe
@JsonTypeName(MoveConfig.NAME)
public final class MoveConfig implements JobConfig {
  private static final long serialVersionUID = -5198319303173120739L;

  public static final String NAME = "Move";

  /** Which block to move. */
  private long mBlockId;

  /** Which medium to move to. */
  private String mMediumType;

  /** worker host containing this block. */
  private String mWorkerHost;

  /**
   * Creates a new instance of {@link MoveConfig}.
   *
   * @param blockId id of the block to move
   * @param workerHost host name of the worker
   * @param mediumType the medium type to move to
   */
  @JsonCreator
  public MoveConfig(@JsonProperty("blockId") long blockId,
      @JsonProperty("workerHost") String workerHost,
      @JsonProperty("mediumType") String mediumType) {
    mBlockId = blockId;
    mMediumType = mediumType;
    mWorkerHost = workerHost;
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * @return the block ID for this job
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the medium type of this move job
   */
  public String getMediumType() {
    return mMediumType;
  }

  /**
   * @return the worker host
   */
  public String getWorkerHost() {
    return mWorkerHost;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MoveConfig)) {
      return false;
    }
    MoveConfig that = (MoveConfig) obj;
    return Objects.equal(mBlockId, that.mBlockId)
        && Objects.equal(mWorkerHost, that.mWorkerHost)
        && Objects.equal(mMediumType, that.mMediumType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockId, mMediumType, mWorkerHost);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("blockId", mBlockId)
        .add("mediumType", mMediumType)
        .add("workerHost", mWorkerHost)
        .toString();
  }
}
