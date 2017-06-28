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

package alluxio.wire;

import alluxio.annotation.PublicApi;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The lineage information.
 */
@PublicApi
@NotThreadSafe
public final class LineageInfo implements Serializable {
  private static final long serialVersionUID = -6407439859190160616L;

  private long mId;
  private ArrayList<String> mInputFiles = new ArrayList<>();
  private ArrayList<String> mOutputFiles = new ArrayList<>();
  private CommandLineJobInfo mJob = new CommandLineJobInfo();
  private long mCreationTimeMs;
  private ArrayList<Long> mParents = new ArrayList<>();
  private ArrayList<Long> mChildren = new ArrayList<>();

  /**
   * Creates a new instance of {@link LineageInfo}.
   */
  public LineageInfo() {}

  /**
   * Creates a new instance of {@link LineageInfo} from a thrift representation.
   *
   * @param lineageInfo the thrift representation of a lineage information
   */
  protected LineageInfo(alluxio.thrift.LineageInfo lineageInfo) {
    mId = lineageInfo.getId();
    mInputFiles = new ArrayList<>(lineageInfo.getInputFiles());
    mOutputFiles = new ArrayList<>(lineageInfo.getOutputFiles());
    mJob = new CommandLineJobInfo(lineageInfo.getJob());
    mCreationTimeMs = lineageInfo.getCreationTimeMs();
    mParents = new ArrayList<>(lineageInfo.getParents());
    mChildren = new ArrayList<>(lineageInfo.getChildren());
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
  public long getCreationTimeMs() {
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
   * @return the lineage information
   */
  public LineageInfo setId(long id) {
    mId = id;
    return this;
  }

  /**
   * @param inputFiles the input files to use
   * @return the lineage information
   */
  public LineageInfo setInputFiles(List<String> inputFiles) {
    Preconditions.checkNotNull(inputFiles, "inputFiles");
    mInputFiles = new ArrayList<>(inputFiles);
    return this;
  }

  /**
   * @param outputFiles the output files to use
   * @return the lineage information
   */
  public LineageInfo setOutputFiles(List<String> outputFiles) {
    Preconditions.checkNotNull(outputFiles, "outputFiles");
    mOutputFiles = new ArrayList<>(outputFiles);
    return this;
  }

  /**
   * @param job the command-line job to use
   * @return the lineage information
   */
  public LineageInfo setJob(CommandLineJobInfo job) {
    Preconditions.checkNotNull(job, "job");
    mJob = job;
    return this;
  }

  /**
   * @param creationTimeMs the creation time (in milliseconds) to use
   * @return the lineage information
   */
  public LineageInfo setCreationTimeMs(long creationTimeMs) {
    mCreationTimeMs = creationTimeMs;
    return this;
  }

  /**
   * @param parents the lineage parents
   * @return the lineage information
   */
  public LineageInfo setParents(List<Long> parents) {
    Preconditions.checkNotNull(parents, "parents");
    mParents = new ArrayList<>(parents);
    return this;
  }

  /**
   * @param children the lineage children
   * @return the lineage information
   */
  public LineageInfo setChildren(List<Long> children) {
    Preconditions.checkNotNull(children, "children");
    mChildren = new ArrayList<>(children);
    return this;
  }

  /**
   * @return thrift representation of the lineage information
   */
  protected alluxio.thrift.LineageInfo toThrift() {
    return new alluxio.thrift.LineageInfo(mId, mInputFiles, mOutputFiles, mJob.toThrift(),
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
    return mId == that.mId && mInputFiles.equals(that.mInputFiles)
        && mOutputFiles.equals(that.mOutputFiles) && mJob.equals(that.mJob)
        && mCreationTimeMs == that.mCreationTimeMs && mParents.equals(that.mParents)
        && mChildren.equals(that.mChildren);
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
