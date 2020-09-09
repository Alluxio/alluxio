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

package alluxio.job.plan.replicate;

import alluxio.job.plan.PlanConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.Collections;

/**
 * Configuration of a job evicting a block.
 */
@ThreadSafe
@JsonTypeName(EvictConfig.NAME)
public final class EvictConfig implements PlanConfig {
  private static final long serialVersionUID = 931006961650512841L;
  public static final String NAME = "Evict";

  private String mPath;

  /** Which block to evict. */
  private long mBlockId;

  /** How many replicas to evict. */
  private int mReplicas;

  /**
   * Creates a new instance of {@link EvictConfig}.
   *
   * @param path alluxio path involved in this eviction
   * @param blockId id of the block to evict
   * @param replicas number of replicas to evict
   */
  @JsonCreator
  public EvictConfig(@JsonProperty("path") String path,
      @JsonProperty("blockId") long blockId,
      @JsonProperty("replicas") int replicas) {
    Preconditions.checkArgument(replicas > 0, "replicas must be positive.");
    mPath = path;
    mBlockId = blockId;
    mReplicas = replicas;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Collection<String> affectedPaths() {
    if (StringUtils.isEmpty(mPath)) {
      return ImmutableList.of(mPath);
    }
    return Collections.EMPTY_LIST;
  }

  /**
   * @return the block ID for this job
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return how many existing blocks to evict
   */
  public int getReplicas() {
    return mReplicas;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof EvictConfig)) {
      return false;
    }
    EvictConfig that = (EvictConfig) obj;
    return Objects.equal(mBlockId, that.mBlockId) && Objects.equal(mReplicas, that.mReplicas);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockId, mReplicas);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("blockId", mBlockId)
        .add("replicas", mReplicas)
        .toString();
  }
}
