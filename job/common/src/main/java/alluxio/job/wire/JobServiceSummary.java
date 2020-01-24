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

import alluxio.job.ProtoUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Job service summary.
 */
@NotThreadSafe
public final class JobServiceSummary {
  public static final int RECENT_LENGTH = 10;

  private final List<StatusSummary> mSummaryPerStatus;

  private final List<JobInfo> mRecentActivities;
  private final List<JobInfo> mRecentFailures;
  private final List<JobInfo> mLongestRunning;

  /**
   * Constructs a new instance of {@link JobServiceSummary} from a
   * collection of {@link JobInfo} it possesses.
   *
   * @param jobInfos Collection of {@link JobInfo}
   */
  public JobServiceSummary(List<JobInfo> jobInfos) {
    jobInfos.sort(Comparator.comparing(JobInfo::getLastUpdated).reversed());
    mSummaryPerStatus = buildSummaryPerStatus(jobInfos);

    mRecentActivities = jobInfos.stream().limit(RECENT_LENGTH).collect(Collectors.toList());

    mRecentFailures = jobInfos.stream().filter(jobInfo -> jobInfo.getStatus().equals(Status.FAILED))
      .limit(RECENT_LENGTH).collect(Collectors.toList());

    Collections.reverse(jobInfos);
    mLongestRunning = jobInfos.stream()
      .filter(jobInfo -> jobInfo.getStatus().equals(Status.RUNNING))
      .limit(RECENT_LENGTH).collect(Collectors.toList());
  }

  /**
   * Constructs a new instance of {@link JobServiceSummary} from a proto object.
   *
   * @param jobServiceSummary the proto object
   */
  public JobServiceSummary(alluxio.grpc.JobServiceSummary jobServiceSummary) throws IOException {
    mSummaryPerStatus = new ArrayList<>();
    for (alluxio.grpc.StatusSummary statusSummary : jobServiceSummary.getSummaryPerStatusList()) {
      mSummaryPerStatus.add(new StatusSummary(statusSummary));
    }

    mRecentActivities = new ArrayList<>();
    for (alluxio.grpc.JobInfo lastActivity : jobServiceSummary.getRecentActivitiesList()) {
      mRecentActivities.add(ProtoUtils.fromProto(lastActivity));
    }

    mRecentFailures = new ArrayList<>();
    for (alluxio.grpc.JobInfo lastFailure : jobServiceSummary.getRecentFailuresList()) {
      mRecentFailures.add(ProtoUtils.fromProto(lastFailure));
    }

    mLongestRunning = new ArrayList<>();
    for (alluxio.grpc.JobInfo longestRunning : jobServiceSummary.getLongestRunningList()) {
      mLongestRunning.add(ProtoUtils.fromProto(longestRunning));
    }
  }

  private List<StatusSummary> buildSummaryPerStatus(List<JobInfo> jobInfos) {

    Map<Status, Long> countPerStatus = new HashMap<>();

    for (JobInfo jobInfo : jobInfos) {
      Status status = Status.valueOf(jobInfo.getStatus().name());
      countPerStatus.compute(status, (key, val) -> (val == null) ? 1 : val + 1);
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
   * @return collection of {@link JobInfo} where the status was most recently updated
   */
  public List<JobInfo> getRecentActivities() {
    return Collections.unmodifiableList(mRecentActivities);
  }

  /**
   * @return collection of {@link JobInfo} that have most recently failed
   */
  public List<JobInfo> getRecentFailures() {
    return Collections.unmodifiableList(mRecentFailures);
  }

  /**
   * @return collection of {@link JobInfo}
   */
  public List<JobInfo> getLongestRunning() {
    return Collections.unmodifiableList(mLongestRunning);
  }

  /**
   * @return proto representation of the job service summary
   * @throws IOException if serialization fails
   */
  public alluxio.grpc.JobServiceSummary toProto() throws IOException {
    alluxio.grpc.JobServiceSummary.Builder jobServiceBuilder =
          alluxio.grpc.JobServiceSummary.newBuilder();

    for (StatusSummary statusSummary : mSummaryPerStatus) {
      jobServiceBuilder.addSummaryPerStatus(statusSummary.toProto());
    }

    for (JobInfo jobInfo : mRecentActivities) {
      jobServiceBuilder.addRecentActivities(jobInfo.toProto());
    }

    for (JobInfo jobInfo : mRecentFailures) {
      jobServiceBuilder.addRecentFailures(jobInfo.toProto());
    }

    for (JobInfo jobInfo : mLongestRunning) {
      jobServiceBuilder.addLongestRunning(jobInfo.toProto());
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
    return Objects.equal(mSummaryPerStatus, that.mSummaryPerStatus)
        && Objects.equal(mRecentActivities, that.mRecentActivities)
        && Objects.equal(mRecentFailures, that.mRecentFailures)
        && Objects.equal(mLongestRunning, that.mLongestRunning);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSummaryPerStatus);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("summaryPerStatus", mSummaryPerStatus)
        .add("recentActivities", mRecentActivities)
        .add("recentFailures", mRecentFailures)
        .add("oldRunning", mLongestRunning)
        .toString();
  }
}

