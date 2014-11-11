package tachyon.worker.hierarchy;

/**
 * Different types of AllocationStrategy, which are used to allocate space among StorageDirs
 */
public enum AllocateStrategyType {
  /**
   * Allocate space on StorageDir that has max free space
   */
  MAX_FREE,
  /**
   * Allocate space on StorageDirs randomly
   */
  RANDOM,
  /**
   * Allocate space on StorageDirs by round robin
   */
  ROUND_ROBIN;
}
