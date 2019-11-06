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

package alluxio.job.plan.transform;

import alluxio.job.plan.PlanConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Configuration for a job to compact files directly under a directory.
 *
 * Files will be compacted into a certain number of files,
 * if the number of existing files is less than the specified number, then no compaction happens,
 * otherwise, assume we want to compact 100 files to 10 files, then every 10 files will be
 * compacted into one file.
 * The original order of rows is preserved.
 */
@ThreadSafe
public final class CompactConfig implements PlanConfig {
  private static final long serialVersionUID = -3434270994964559796L;

  private static final String NAME = "Compact";

  private final PartitionInfo mPartitionInfo;
  /**
   * Files directly under this directory are compacted.
   */
  private final String mInput;
  /**
   * Compacted files are stored under this directory.
   */
  private final String mOutput;
  /**
   * The type of database to write the compacted table to.
   */
  private final String mDatabaseType;
  /**
   * Number of files after compaction.
   */
  private final int mNumFiles;

  /**
   * @param partitionInfo the partition info
   * @param input the input directory
   * @param output the output directory
   * @param databaseType the type of database to write the compacted table to
   * @param numFiles the number of files after compaction
   */
  public CompactConfig(@JsonProperty("partitionInfo") PartitionInfo partitionInfo,
      @JsonProperty("input") String input,
      @JsonProperty("output") String output,
      @JsonProperty("databaseType") String databaseType,
      @JsonProperty("numFiles") Integer numFiles) {
    mPartitionInfo = partitionInfo;
    mInput = Preconditions.checkNotNull(input, "input");
    mOutput = Preconditions.checkNotNull(output, "output");
    mDatabaseType = Preconditions.checkNotNull(databaseType, "databaseType");
    mNumFiles = Preconditions.checkNotNull(numFiles, "numFiles");
  }

  /**
   * @return the partition info
   */
  public PartitionInfo getPartitionInfo() {
    return mPartitionInfo;
  }

  /**
   * @return the input directory
   */
  public String getInput() {
    return mInput;
  }

  /**
   * @return the output directory
   */
  public String getOutput() {
    return mOutput;
  }

  /**
   * @return the type of database to write the compacted table to
   */
  public String getDatabaseType() {
    return mDatabaseType;
  }

  /**
   * @return the number of files after compaction
   */
  public int getNumFiles() {
    return mNumFiles;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof CompactConfig)) {
      return false;
    }
    CompactConfig that = (CompactConfig) obj;
    return mPartitionInfo.equals(that.mPartitionInfo)
        && mInput.equals(that.mInput)
        && mDatabaseType.equals(that.mDatabaseType)
        && mOutput.equals(that.mOutput)
        && mNumFiles == that.mNumFiles;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPartitionInfo, mInput, mOutput, mDatabaseType, mNumFiles);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("partitionInfo", mPartitionInfo)
        .add("input", mInput)
        .add("output", mOutput)
        .add("databaseType", mDatabaseType)
        .add("numFiles", mNumFiles)
        .toString();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
