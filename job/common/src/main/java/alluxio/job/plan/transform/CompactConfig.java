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
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;

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

  private final PartitionInfo mInputPartitionInfo;
  /**
   * Files directly under this directory are compacted.
   */
  private final String mInput;

  private final PartitionInfo mOutputPartitionInfo;
  /**
   * Compacted files are stored under this directory.
   */
  private final String mOutput;
  /**
   * Max number of files after compaction.
   */
  private final int mMaxNumFiles;
  /**
   * Minimum file size for compaction.
   */
  private final long mMinFileSize;

  /**
   * @param inputPartitionInfo the input partition info
   * @param input the input directory
   * @param outputPartitionInfo the output partition info
   * @param output the output directory
   * @param maxNumFiles the maximum number of files after compaction
   * @param minFileSize the minimum file size for coalescing
   */
  public CompactConfig(@JsonProperty("inputPartitionInfo") PartitionInfo inputPartitionInfo,
                       @JsonProperty("input") String input,
                       @JsonProperty("outputPartitionInfo") PartitionInfo outputPartitionInfo,
                       @JsonProperty("output") String output,
                       @JsonProperty("maxNumFiles") Integer maxNumFiles,
                       @JsonProperty("minFileSize") Long minFileSize) {
    mInputPartitionInfo = inputPartitionInfo;
    mInput = Preconditions.checkNotNull(input, "input");
    mOutputPartitionInfo = outputPartitionInfo;
    mOutput = Preconditions.checkNotNull(output, "output");
    mMaxNumFiles = Preconditions.checkNotNull(maxNumFiles, "maxNumFiles");
    mMinFileSize = Preconditions.checkNotNull(minFileSize, "minFileSize");
  }

  /**
   * @return the input partition info
   */
  public PartitionInfo getInputPartitionInfo() {
    return mInputPartitionInfo;
  }

  /**
   * @return the output partition info
   */
  public PartitionInfo getOutputPartitionInfo() {
    return mOutputPartitionInfo;
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
   * @return the number of files after compaction
   */
  public int getMaxNumFiles() {
    return mMaxNumFiles;
  }

  /**
   * @return the file size
   */
  public long getMinFileSize() {
    return mMinFileSize;
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
    return mInputPartitionInfo.equals(that.mInputPartitionInfo)
        && mOutputPartitionInfo.equals(that.mOutputPartitionInfo)
        && mInput.equals(that.mInput)
        && mOutput.equals(that.mOutput)
        && mMaxNumFiles == that.mMaxNumFiles
        && mMinFileSize == that.mMinFileSize;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mInputPartitionInfo, mOutputPartitionInfo, mInput, mOutput,
        mMaxNumFiles, mMinFileSize);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("input", mInput)
        .add("output", mOutput)
        .add("maxNumFiles", mMaxNumFiles)
        .add("minFileSize", mMinFileSize)
        .add("inputPartitionInfo", mInputPartitionInfo)
        .add("outputPartitionInfo", mOutputPartitionInfo)
        .toString();
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Collection<String> affectedPaths() {
    return ImmutableList.of(mInput, mOutput);
  }
}
