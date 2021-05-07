package alluxio.master.block.meta;

import alluxio.StorageTierAssoc;

import java.util.List;
import java.util.Map;

public class WorkerUsageMeta {
  /** Capacity of worker in bytes. */
  private long mCapacityBytes;
  /** Worker's used bytes. */
  private long mUsedBytes;
  /** Worker-specific mapping between storage tier alias and storage tier ordinal. */
  private StorageTierAssoc mStorageTierAssoc;
  /** Mapping from storage tier alias to total bytes. */
  private Map<String, Long> mTotalBytesOnTiers;
  /** Mapping from storage tier alias to used bytes. */
  private Map<String, Long> mUsedBytesOnTiers;

  /** Mapping from tier alias to lost storage paths. */
  private Map<String, List<String>> mLostStorage;
}
