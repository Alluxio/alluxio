package alluxio.job.plan.replicate;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * A task representing loading a block into the memory of a worker.
 */
public final class SetReplicaTask implements Serializable {
  private static final long serialVersionUID = 2028545900913354425L;
  final Mode mMode;

  /**
   * @param mode evict or replicate
   */
  public SetReplicaTask(Mode mode) {
    mMode = mode;
  }

  /**
   * @return the block id
   */
  public Mode getMode() {
    return mMode;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("mode", mMode).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SetReplicaTask)) {
      return false;
    }
    SetReplicaTask that = (SetReplicaTask) o;
    return Objects.equal(mMode, that.mMode);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mMode);
  }
}
