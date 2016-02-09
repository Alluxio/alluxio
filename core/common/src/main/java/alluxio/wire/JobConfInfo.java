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

package alluxio.wire;

import alluxio.annotation.PublicApi;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The lineage command-line job configuration.
 */
@NotThreadSafe
@PublicApi
// TODO(jiri): Consolidate with alluxio.job.JobConf
public final class JobConfInfo {
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
