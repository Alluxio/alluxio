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

package alluxio.job.migrate;

import alluxio.job.JobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Configuration for the migrate job. A migration can either be a copy or a move.
 * See {@code MigrateDefinition} for detailed semantics.
 */
@ThreadSafe
public class MigrateConfig implements JobConfig {
  private static final long serialVersionUID = 8014674802258120190L;
  private static final String NAME = "Migrate";

  private final String mSource;
  private final String mDestination;
  private final String mWriteType;
  private final boolean mOverwrite;
  private final boolean mDeleteSource;

  /**
   * @param source the source path
   * @param dst the destination path
   * @param writeType the Alluxio write type with which to write the migrated file; a null value
   *        means to use the default write type from the Alluxio configuration
   * @param overwrite whether an existing file should be overwritten; if the source and destination
   *        are directories, the contents of the directories will be merged with common files
   *        overwritten by the source
   * @param deleteSource whether to delete the source path after migration
   */
  public MigrateConfig(@JsonProperty("source") String source,
                       @JsonProperty("destination") String dst,
                       @JsonProperty("writeType") String writeType,
                       @JsonProperty("overwrite") boolean overwrite,
                       @JsonProperty("deleteSource") boolean deleteSource) {
    mSource = Preconditions.checkNotNull(source, "source must be set");
    mDestination = Preconditions.checkNotNull(dst, "destination must be set");
    mWriteType = writeType;
    mOverwrite = overwrite;
    mDeleteSource = deleteSource;
  }

  /**
   * @return the source path
   */
  public String getSource() {
    return mSource;
  }

  /**
   * @return the destination path
   */
  public String getDestination() {
    return mDestination;
  }

  /**
   * @return the writeType
   */
  public String getWriteType() {
    return mWriteType;
  }

  /**
   * @return whether to overwrite a file at the destination if it exists
   */
  public boolean isOverwrite() {
    return mOverwrite;
  }

  /**
   * @return whether to delete the source path after migration
   */
  public boolean isDeleteSource() {
    return mDeleteSource;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MigrateConfig)) {
      return false;
    }
    MigrateConfig that = (MigrateConfig) obj;
    return Objects.equal(mSource, that.mSource)
        && Objects.equal(mDestination, that.mDestination)
        && Objects.equal(mWriteType, that.mWriteType)
        && Objects.equal(mOverwrite, that.mOverwrite)
        && Objects.equal(mDeleteSource, that.mDeleteSource);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSource, mDestination, mWriteType, mOverwrite, mDeleteSource);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", mSource)
        .add("destination", mDestination)
        .add("writeType", mWriteType)
        .add("overwrite", mOverwrite)
        .add("deleteSource", mDeleteSource)
        .toString();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
