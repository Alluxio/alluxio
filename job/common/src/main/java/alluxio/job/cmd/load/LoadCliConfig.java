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

package alluxio.job.cmd.load;

import alluxio.grpc.OperationType;
import alluxio.job.cmd.CliConfig;
import alluxio.job.wire.JobSource;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A config for a LoadCli job.
 */
public class LoadCliConfig implements CliConfig {
  public static final String NAME = "LoadCli";
  private static final long serialVersionUID = -5268683490243619278L;

  private final String mFilePath;
  private final int mReplication;
  private final Set<String> mWorkerSet;
  private final Set<String> mExcludedWorkerSet;
  private final Set<String> mLocalityIds;
  private final Set<String> mExcludedLocalityIds;

  /**
   * @param filePath the file path
   * @param replication the number of workers to store each block on, defaults to 1
   * @param workerSet the worker set
   * @param excludedWorkerSet the excluded worker set
   * @param localityIds the locality identify set
   * @param excludedLocalityIds the excluded locality identify set
   **/
  public LoadCliConfig(@JsonProperty("filePath") String filePath,
                    @JsonProperty("replication") Integer replication,
                    @JsonProperty("workerSet") Set<String> workerSet,
                    @JsonProperty("excludedWorkerSet") Set<String> excludedWorkerSet,
                    @JsonProperty("localityIds") Set<String> localityIds,
                    @JsonProperty("excludedLocalityIds") Set<String> excludedLocalityIds) {
    mFilePath = Preconditions.checkNotNull(filePath, "The file path cannot be null");
    mReplication = replication == null ? 1 : replication;
    mWorkerSet = workerSet == null ? Collections.EMPTY_SET : new HashSet(workerSet);
    mExcludedWorkerSet = excludedWorkerSet == null ? Collections.EMPTY_SET
        : new HashSet(excludedWorkerSet);
    mLocalityIds = localityIds == null ? Collections.EMPTY_SET : new HashSet(localityIds);
    mExcludedLocalityIds = excludedLocalityIds == null ? Collections.EMPTY_SET
        : new HashSet(excludedLocalityIds);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public JobSource getJobSource() {
    return JobSource.CLI;
  }

  @Override
  public OperationType getOperationType() {
    return OperationType.DIST_LOAD;
  }

  @Override
  public Collection<String> affectedPaths() {
    return ImmutableList.of(mFilePath);
  }

  /**
   * @return worker set
   */
  public Set<String> getWorkerSet() {
    return mWorkerSet;
  }

  /**
   * @return excluded worker set
   */
  public Set<String> getExcludedWorkerSet() {
    return mExcludedWorkerSet;
  }

  /**
   * @return locality identify set
   */
  public Set<String> getLocalityIds() {
    return mLocalityIds;
  }

  /**
   * @return excluded locality identify set
   */
  public Set<String> getExcludedLocalityIds() {
    return mExcludedLocalityIds;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof LoadCliConfig)) {
      return false;
    }
    LoadCliConfig that = (LoadCliConfig) obj;
    return mFilePath.equals(that.mFilePath) && mReplication == that.mReplication;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mFilePath, mReplication);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("FilePath", mFilePath)
            .add("Replication", mReplication)
            .toString();
  }
}
