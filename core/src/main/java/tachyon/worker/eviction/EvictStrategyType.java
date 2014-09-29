package tachyon.worker.eviction;

/**
 * Different types of EvictionStrategy, currently LRU based strategies are implemented
 */
public enum EvictStrategyType {
  /**
   * Evict old blocks among several StorageDirs by LRU
   */
  LRU,
  /**
   * Evict old blocks in certain StorageDir by LRU.
   */
  PARTIAL_LRU;
}
