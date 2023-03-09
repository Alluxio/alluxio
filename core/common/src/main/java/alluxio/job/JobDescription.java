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

package alluxio.job;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 *  Job description that used as a key to identify the job in the scheduler.
 */
public class JobDescription {

  private final String mPath;
  private final String mType;

  private JobDescription(String type, String path) {
    mPath = path;
    mType = type;
  }

  /**
   * @return the path of the job affected
   */
  public String getType() {
    return mType;
  }

  /**
   * @return the type of the job
   */
  public String getPath() {
    return mPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JobDescription that = (JobDescription) o;
    return Objects.equal(mPath, that.mPath) && Objects.equal(mType, that.mType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mType, mPath);
  }

  @Override
  public String toString() {
    return MoreObjects
        .toStringHelper(this)
        .add("Path", mPath)
        .add("Type", mType)
        .toString();
  }

  /**
   * create a job description from JobDescription proto.
   * @param jobDescription JobDescription proto
   * @return job description
   */
  public static JobDescription from(alluxio.grpc.JobDescription jobDescription) {
    return new JobDescription(jobDescription.getType(), jobDescription.getPath());
  }

  /**
   * @return the job description builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for {@link JobDescription}.
   */
  public static class Builder {
    private String mPath;
    private String mType;

    private Builder() {}

    /**
     * set path.
     * @param path affected path
     * @return builder
     */
    public Builder setPath(String path) {
      mPath = path;
      return this;
    }

    /**
     * set job type.
     * @param type job type
     * @return builder
     */
    public Builder setType(String type) {
      mType = type;
      return this;
    }

    /**
     * build job description.
     * @return job description
     */
    public JobDescription build() {
      return new JobDescription(mType, mPath);
    }
  }
}
