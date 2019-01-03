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

package alluxio.job.load;

import alluxio.job.JobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The configuration of loading a file.
 */
@ThreadSafe
public class LoadConfig implements JobConfig {
  public static final String NAME = "Load";

  private static final long serialVersionUID = -7937106659935180792L;
  private final String mFilePath;
  private final int mReplication;

  /**
   * @param filePath the file path
   * @param replication the number of workers to store each block on, defaults to 1
   */
  public LoadConfig(@JsonProperty("filePath") String filePath,
      @JsonProperty("replication") Integer replication) {
    mFilePath = Preconditions.checkNotNull(filePath, "The file path cannot be null");
    mReplication = replication == null ? 1 : replication;
  }

  /**
   * @return the file path
   */
  public String getFilePath() {
    return mFilePath;
  }

  /**
   * @return the number of workers to store each block on
   */
  public int getReplication() {
    return mReplication;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof LoadConfig)) {
      return false;
    }
    LoadConfig that = (LoadConfig) obj;
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

  @Override
  public String getName() {
    return NAME;
  }
}
