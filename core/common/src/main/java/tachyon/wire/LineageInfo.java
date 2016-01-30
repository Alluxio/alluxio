/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.wire;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * The lineage descriptor.
 */
public class LineageInfo {
  @JsonProperty("id")
  private long mId;
  @JsonProperty("inputFiles")
  private List<String> mInputFiles;
  @JsonProperty("outputFiles")
  private List<String> mOutputFiles;
  @JsonProperty("job")
  private CommandLineJobInfo mJob;
  @JsonProperty("creationTimeMs")
  private long mCreationTimeMs;
  @JsonProperty("parents")
  private List<Long> mParents;
  @JsonProperty("children")
  private List<Long> mChildren;

  /**
   * Creates a new instance of {@link LineageInfo}.
   */
  public LineageInfo() {
  }

  /**
   * Creates a new instance of {@link LineageInfo} from a thrift representation.
   *
   * @param lineageInfo the thrift representation of a lineage descriptor
   */
  public LineageInfo(tachyon.thrift.LineageInfo lineageInfo) {
    mId = lineageInfo.getId();
    mInputFiles = lineageInfo.getInputFiles();
    mOutputFiles = lineageInfo.getOutputFiles();
    mJob = new CommandLineJobInfo(lineageInfo.getJob());
    mCreationTimeMs = lineageInfo.getCreationTimeMs();
    mParents = lineageInfo.getParents();
    mChildren = lineageInfo.getChildren();
  }

  /**
   * @return the lineage id
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the lineage input files
   */
  public List<String> getInputFiles() {
    return mInputFiles;
  }

  /**
   * @return the lineage output files
   */
  public List<String> getOutputFiles() {
    return mOutputFiles;
  }

  /**
   * @return the lineage command-line job
   */
  public CommandLineJobInfo getJob() {
    return mJob;
  }

  /**
   * @return the lineage creation time (in milliseconds)
   */
  public long getCreationTimesMs() {
    return mCreationTimeMs;
  }

  /**
   * @return the lineage parents
   */
  public List<Long> getParents() {
    return mParents;
  }

  /**
   * @return the lineage children
   */
  public List<Long> getChildren() {
    return mChildren;
  }

  /**
   * @param id the lineage id to use
   * @return the lineage
   */
  public LineageInfo setId(long id) {
    mId = id;
    return this;
  }

  /**
   * @param inputFiles the input files to use
   * @return the lineage
   */
  public LineageInfo setInputFiles(List<String> inputFiles) {
    mInputFiles = inputFiles;
    return this;
  }

  /**
   * @param outputFiles the output files to use
   * @return the lineage
   */
  public LineageInfo setOutputFiles(List<String> outputFiles) {
    mOutputFiles = outputFiles;
    return this;
  }

  /**
   * @param job the command-line job to use
   * @return the lineage
   */
  public LineageInfo setJob(CommandLineJobInfo job) {
    mJob = job;
    return this;
  }

  /**
   * @param creationTimeMs the creation time (in milliseconds) to use
   * @return the lineage
   */
  public LineageInfo setCreationTimeMs(long creationTimeMs) {
    mCreationTimeMs = creationTimeMs;
    return this;
  }

  /**
   * @param parents the lineage parents
   * @return the lineage
   */
  public LineageInfo setParents(List<Long> parents) {
    mParents = parents;
    return this;
  }

  /**
   * @param children the lineage children
   * @return the lineage
   */
  public LineageInfo setChildren(List<Long> children) {
    mChildren = children;
    return this;
  }

  /**
   * @return thrift representation of the lineage descriptor
   */
  public tachyon.thrift.LineageInfo toThrift() {
    return new tachyon.thrift.LineageInfo(mId, mInputFiles, mOutputFiles, mJob.toThrift(),
        mCreationTimeMs, mParents, mChildren);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LineageInfo)) {
      return false;
    }
    LineageInfo that = (LineageInfo) o;
    return mId == that.mId
        && ((mInputFiles == null && that.mInputFiles == null) || mInputFiles
            .equals(that.mInputFiles))
        && ((mOutputFiles == null && that.mOutputFiles == null) || mOutputFiles
            .equals(that.mOutputFiles))
        && ((mJob == null && that.mJob == null) || mJob.equals(that.mJob))
        && mCreationTimeMs == that.mCreationTimeMs
        && ((mParents == null && that.mParents == null) || mParents.equals(that.mParents))
        && ((mChildren == null && that.mChildren == null) || mChildren.equals(that.mChildren));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId, mInputFiles, mOutputFiles, mJob, mCreationTimeMs, mParents,
        mChildren);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("id", mId).add("inputFiles", mInputFiles)
        .add("outputFiles", mOutputFiles).add("job", mJob).add("creationTimeMs", mCreationTimeMs)
        .add("parents", mParents).add("children", mChildren).toString();
  }
}
