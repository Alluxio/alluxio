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

package alluxio.job.plan.persist;

import alluxio.job.plan.PlanConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;

/**
 * The configuration of persisting a file.
 */
@ThreadSafe
@JsonTypeName(PersistConfig.NAME)
public class PersistConfig implements PlanConfig {
  private static final long serialVersionUID = -404303102995033014L;

  public static final String NAME = "Persist";

  /** The path for the Alluxio file to persist. */
  private String mFilePath;
  /** The mount ID for the UFS path to persist the file to. */
  private long mMountId;
  /** Determines whether to overwrite an existing file in UFS. */
  private final boolean mOverwrite;
  /** Determines the UFS path to persist the file to. */
  private String mUfsPath;

  /**
   * Creates a new instance of {@link PersistConfig}.
   *
   * @param filePath the Alluxio path of the file to persist
   * @param mountId the mount ID for the UFS path to persist the file to
   * @param overwrite flag of overwriting the existing file in UFS or not
   * @param ufsPath the UFS path to persist the file to
   */
  @JsonCreator
  public PersistConfig(@JsonProperty("filePath") String filePath,
      @JsonProperty("mountId") long mountId, @JsonProperty("overwrite") boolean overwrite,
      @JsonProperty("ufsPath") String ufsPath) {
    mFilePath = Preconditions.checkNotNull(filePath, "The file path cannot be null");
    mMountId = Preconditions.checkNotNull(mountId, "The mount ID cannot be null");
    mOverwrite = overwrite;
    mUfsPath = Preconditions.checkNotNull(ufsPath, "The UFS path cannot be null");
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * @return the file path
   */
  public String getFilePath() {
    return mFilePath;
  }

  /**
   * @return the mount ID
   */
  public long getMountId() {
    return mMountId;
  }

  /**
   * @return the UFS path
   */
  public String getUfsPath() {
    return mUfsPath;
  }

  /**
   * @return flag of overwriting the existing file in under storage or not
   */
  public boolean isOverwrite() {
    return mOverwrite;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof PersistConfig)) {
      return false;
    }
    PersistConfig that = (PersistConfig) obj;
    return Objects.equal(mFilePath, that.mFilePath)
        && Objects.equal(mMountId, that.mMountId)
        && Objects.equal(mOverwrite, that.mOverwrite)
        && Objects.equal(mUfsPath, that.mUfsPath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mFilePath, mMountId, mOverwrite, mUfsPath);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("filePath", mFilePath).add("mountId", mMountId)
        .add("overwrite", mOverwrite).add("ufsPath", mUfsPath).toString();
  }

  @Override
  public Collection<String> affectedPaths() {
    return ImmutableList.of(mFilePath);
  }
}
