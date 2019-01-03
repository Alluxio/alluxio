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

package alluxio.job.move;

import alluxio.job.JobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Configuration for the move job. See {@code MoveDefinition} for detailed semantics.
 */
@ThreadSafe
public class MoveConfig implements JobConfig {
  public static final String NAME = "Move";

  private static final long serialVersionUID = 2249161137868881346L;

  private final String mSource;
  private final String mDestination;
  private final String mWriteType;
  private final boolean mOverwrite;

  /**
   * @param source the source path
   * @param dst the destination path
   * @param writeType the Alluxio write type with which to write the moved file; a null value means
   *        to use the default write type from the Alluxio configuration
   * @param overwrite whether an existing file should be overwritten; if the source and destination
   *        are directories, the contents of the directories will be merged with common files
   *        overwritten by the source
   */
  public MoveConfig(@JsonProperty("source") String source, @JsonProperty("destination") String dst,
      @JsonProperty("writeType") String writeType, @JsonProperty("overwrite") boolean overwrite) {
    mSource = Preconditions.checkNotNull(source, "source must be set");
    mDestination = Preconditions.checkNotNull(dst, "destination must be set");
    mWriteType = writeType;
    mOverwrite = overwrite;
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
    return Objects.equal(mSource, that.mSource)
        && Objects.equal(mDestination, that.mDestination)
        && Objects.equal(mWriteType, that.mWriteType)
        && Objects.equal(mOverwrite, that.mOverwrite);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSource, mDestination, mWriteType, mOverwrite);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", mSource)
        .add("destination", mDestination)
        .add("writeType", mWriteType)
        .add("overwrite", mOverwrite)
        .toString();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
