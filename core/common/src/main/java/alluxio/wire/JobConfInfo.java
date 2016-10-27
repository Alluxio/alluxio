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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The lineage command-line job configuration.
 */
@PublicApi
@NotThreadSafe
// TODO(jiri): Consolidate with alluxio.job.JobConf
public final class JobConfInfo implements Serializable {
  private static final long serialVersionUID = -7905777467753059106L;

  private String mOutputFile = "";

  /**
   * Creates a new instance of {@link JobConfInfo}.
   */
  public JobConfInfo() {}

  /**
   * Creates a new instance of {@link JobConfInfo} from a thrift representation.
   *
   * @param jobConfInfo the thrift representation of a lineage command-line job configuration
   */
  protected JobConfInfo(alluxio.thrift.JobConfInfo jobConfInfo) {
    mOutputFile = jobConfInfo.getOutputFile();
  }

  /**
   * @return the output file
   */
  public String getOutputFile() {
    return mOutputFile;
  }

  /**
   * @param outputFile the output file to use
   * @return the lineage command-line job configuration
   */
  public JobConfInfo setOutputFile(String outputFile) {
    Preconditions.checkNotNull(outputFile);
    mOutputFile = outputFile;
    return this;
  }

  /**
   * @return thrift representation of the lineage command-line job configuration
   */
  protected alluxio.thrift.JobConfInfo toThrift() {
    return new alluxio.thrift.JobConfInfo(mOutputFile);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JobConfInfo)) {
      return false;
    }
    JobConfInfo that = (JobConfInfo) o;
    return mOutputFile.equals(that.mOutputFile);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mOutputFile);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("outputFile", mOutputFile).toString();
  }
}
