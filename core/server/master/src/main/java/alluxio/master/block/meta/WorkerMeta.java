package alluxio.master.block.meta;

import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.Preconditions;
import net.jcip.annotations.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class WorkerMeta {
  /** Worker's address. */
  public final WorkerNetAddress mWorkerAddress;
  /** The id of the worker. */
  public final long mId;
  /** Start time of the worker in ms. */
  public final long mStartTimeMs;
  /** Worker's last updated time in ms. */
  public final AtomicLong mLastUpdatedTimeMs;

  public WorkerMeta(long id, WorkerNetAddress address) {
    mId = id;
    mWorkerAddress = Preconditions.checkNotNull(address, "address");
    mStartTimeMs = CommonUtils.getCurrentMs();
    mLastUpdatedTimeMs = new AtomicLong(CommonUtils.getCurrentMs());
  }

  public void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs.set(CommonUtils.getCurrentMs());
  }
}
