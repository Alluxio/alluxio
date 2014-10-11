package tachyon.worker.eviction;

/**
 * Used to get specific EvictStrategy by EvictStrategyType
 */
public class EvictStrategies {
  /**
   * Get block EvictStrategy based on configuration
   * 
   * @param evictStrategy configuration of EvictStrategy
   * @param isLastTier whether eviction is on last StorageTier
   * @return EvictStrategy generated
   */
  public static EvictStrategy getEvictStrategy(EvictStrategyType strategyType, boolean isLastTier) {
    switch (strategyType) {
      case LRU:
        return new EvictLRU(isLastTier);
      case PARTIAL_LRU:
        return new EvictPartialLRU(isLastTier);
      default:
        return new EvictLRU(isLastTier);
    }
  }

  private EvictStrategies() {}
}
