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

import alluxio.worker.block.BlockStoreLocation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Configuration of a job evicting a block.
 */
@ThreadSafe
@JsonTypeName(MoveConfig.NAME)
public final class MoveConfig implements JobConfig {
  public static final String NAME = "Move";

  /** Which block to move. */
  private long mBlockId;

  /** source of the move. */
  private BlockStoreLocation mSource;

  /** destination of the move. */
  private BlockStoreLocation mDestination;

  /** worker host containing this block. */
  private String mWorkerHost;

  /**
   * Creates a new instance of {@link MoveConfig}.
   *
   * @param blockId id of the block to move
   * @param source source to move from
   * @param destination destination to move to
   */
  @JsonCreator
  public MoveConfig(@JsonProperty("blockId") long blockId,
      @JsonProperty("source") BlockStoreLocation source,
      @JsonProperty("destination") BlockStoreLocation destination,
      @JsonProperty("workerHost") String workerHost) {
    mBlockId = blockId;
    mSource = source;
    mDestination = destination;
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
   * @return the source for this move job
   */
  public BlockStoreLocation getSource() {
    return mSource;
  }

  /**
   * @return the destination for this move job
   */
  public BlockStoreLocation getDestination() {
    return mDestination;
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
    return Objects.equal(mBlockId, that.mBlockId) && Objects.equal(mSource, that.mSource)
        && Objects.equal(mDestination, that.mDestination)
        && Objects.equal(mWorkerHost, that.mWorkerHost);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockId, mSource, mDestination, mWorkerHost);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("blockId", mBlockId)
        .add("source", mSource)
        .add("destination", mDestination)
        .add("workerHost", mWorkerHost)
        .toString();
  }
}
