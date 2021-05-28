package alluxio.master.block.meta;

/**
 * Enum for the locks in {@link MasterWorkerInfo}.
 * Each lock type corresponds to one group of metadata in the object.
 */
public enum WorkerMetaLockType {
  STATUS_LOCK,
  USAGE_LOCK,
  BLOCKS_LOCK;
}
