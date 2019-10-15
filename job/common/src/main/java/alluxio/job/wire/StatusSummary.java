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

package alluxio.job.wire;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Status summary.
 */
public class StatusSummary {
  private Status mStatus;
  private long mCount;

  /**
   * Constructs a new instance of {@link StatusSummary}
   * from a {@link Status} and number of jobs with that status.
   *
   * @param status possible {@link Status} of a job
   * @param count number of jobs with the particular status
   */
  public StatusSummary(Status status, long count) {
    mStatus = status;
    mCount = count;
  }

  /**
   * Constructs a new instance of {@link StatusSummary} from a proto object.
   *
   * @param statusSummary the proto object
   */
  public StatusSummary(alluxio.grpc.StatusSummary statusSummary) {
    mStatus = Status.valueOf(statusSummary.getStatus().name());
    mCount = statusSummary.getCount();
  }

  /**
   * Returns the {@link Status} this object represents.
   *
   * @return {@link Status}
   */
  public Status getStatus() {
    return mStatus;
  }

  /**
   * Returns the count of the number of jobs associated with {@link Status}.
   *
   * @return the count of the number of jobs associated with {@link Status}
   */
  public long getCount() {
    return mCount;
  }

  /**
   * @return proto representation of the status summary
   */
  public alluxio.grpc.StatusSummary toProto() {
    alluxio.grpc.StatusSummary.Builder jobServiceBuilder = alluxio.grpc.StatusSummary.newBuilder()
          .setStatus(mStatus.toProto()).setCount(mCount);

    return jobServiceBuilder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof StatusSummary)) {
      return false;
    }
    StatusSummary that = (StatusSummary) o;
    return Objects.equal(mStatus, that.mStatus)
            && Objects.equal(mCount, that.mCount);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mStatus, mCount);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("status", mStatus)
            .add("count", mCount)
            .toString();
  }
}
