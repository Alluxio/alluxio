package alluxio.job.wire;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;

/**
 * Job service summary.
 */
@NotThreadSafe
public final class JobServiceSummary {
  private final List<StatusSummary> mSummaryPerStatus;

  public JobServiceSummary(Collection<JobInfo> jobInfos) {
    mSummaryPerStatus = buildSummaryPerStatus(jobInfos);
  }

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
      if (count == null) { count = 0L; }
      result.add(new StatusSummary(status, count));
    }

    return result;
  }

  public Collection<StatusSummary> getSummaryPerStatus() {
    return Collections.unmodifiableCollection(mSummaryPerStatus);
  }

  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
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


