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

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Job service summary.
 */
@NotThreadSafe
public final class JobServiceSummary {
  private final List<StatusSummary> mSummaryPerStatus;

  /**
   * Constructs a new instance of {@link JobServiceSummary} from a
   * collection of {@link JobInfo} it possesses.
   *
   * @param jobInfos Collection of {@link JobInfo}
   */
  public JobServiceSummary(Collection<JobInfo> jobInfos) {
    mSummaryPerStatus = buildSummaryPerStatus(jobInfos);
  }

  /**
   * Constructs a new instance of {@link JobServiceSummary} from a proto object.
   *
   * @param jobServiceSummary the proto object
   */
  public JobServiceSummary(alluxio.grpc.JobServiceSummary jobServiceSummary) {
    mSummaryPerStatus = new ArrayList<>();
    for (alluxio.grpc.StatusSummary statusSummary : jobServiceSummary.getSummaryPerStatusList()) {
      mSummaryPerStatus.add(new StatusSummary(statusSummary));
    }
  }

  private List<StatusSummary> buildSummaryPerStatus(Collection<JobInfo> jobInfos) {

    Map<Status, Long> countPerStatus = new HashMap<>();

    for (JobInfo jobInfo : jobInfos) {
      Status status = Status.valueOf(jobInfo.getStatus().name());
      countPerStatus.putIfAbsent(status, 0L);
      countPerStatus.put(status, countPerStatus.get(status) + 1L);
    }

    List<StatusSummary> result = new ArrayList<>();

    for (Status status : Status.values()) {
      Long count = countPerStatus.get(status);
      if (count == null) {
        count = 0L;
      }
      result.add(new StatusSummary(status, count));
    }

    return result;
  }

  /**
   * Returns an unmodifiable collection of summary per job status.
   *
   * @return collection of summary per job status
   */
  public Collection<StatusSummary> getSummaryPerStatus() {
    return Collections.unmodifiableCollection(mSummaryPerStatus);
  }

  /**
   * @return proto representation of the job service summary
   */
  public alluxio.grpc.JobServiceSummary toProto() {
    alluxio.grpc.JobServiceSummary.Builder jobServiceBuilder =
          alluxio.grpc.JobServiceSummary.newBuilder();
    for (StatusSummary statusSummary : mSummaryPerStatus) {
      jobServiceBuilder.addSummaryPerStatus(statusSummary.toProto());
    }
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
    if (!(o instanceof JobServiceSummary)) {
      return false;
    }
    JobServiceSummary that = (JobServiceSummary) o;
    return Objects.equal(mSummaryPerStatus, that.mSummaryPerStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSummaryPerStatus);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("summaryPerStatus", mSummaryPerStatus)
            .toString();
  }
}

