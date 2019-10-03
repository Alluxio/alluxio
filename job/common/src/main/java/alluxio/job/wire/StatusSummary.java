package alluxio.job.wire;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class StatusSummary {
  private Status mStatus;
  private long mCount;

  public StatusSummary(Status status, long count) {
    mStatus = status;
    mCount = count;
  }

  public StatusSummary(alluxio.grpc.StatusSummary statusSummary) {
    mStatus = Status.valueOf(statusSummary.getStatus().name());
    mCount = statusSummary.getCount();
  }

  public Status getStatus() {
    return mStatus;
  }

  public long getCount() {
    return mCount;
  }

  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
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
