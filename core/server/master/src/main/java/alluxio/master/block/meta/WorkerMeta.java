package alluxio.master.block.meta;

import alluxio.StorageTierAssoc;
import alluxio.wire.WorkerNetAddress;

import java.util.Map;

public class WorkerMeta {
  /** Worker's address. */
  private final WorkerNetAddress mWorkerAddress;
  /** The id of the worker. */
  private final long mId;
  /** Start time of the worker in ms. */
  private final long mStartTimeMs;
  /** Worker's last updated time in ms. */
  private long mLastUpdatedTimeMs;
  /** If true, the worker is considered registered. */
  private boolean mIsRegistered;
}
