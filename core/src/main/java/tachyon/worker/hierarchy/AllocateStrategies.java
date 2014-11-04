package tachyon.worker.hierarchy;

import java.util.Random;

/**
 * Define several AllocateStrategy, and get specific AllocateStrategy by AllocateStrategyType
 */
public class AllocateStrategies {
  /**
   * Allocate space on StorageDir that has max free space
   */
  private static class AllocateMaxFree extends AllocateStrategyBase {
    @Override
    public StorageDir getStorageDir(StorageDir[] storageDirs, long userId, long requestSizeBytes) {
      StorageDir availableDir = null;
      long maxFree = 0;
      while (true) {
        for (StorageDir dir : storageDirs) {
          if (dir.getAvailableBytes() >= maxFree && dir.getAvailableBytes() >= requestSizeBytes) {
            maxFree = dir.getAvailableBytes();
            availableDir = dir;
          }
        }
        if (availableDir == null) {
          return null;
        } else if (availableDir.requestSpace(userId, requestSizeBytes)) {
          return availableDir;
        }
      }
    }
  }

  /**
   * Allocate space on StorageDirs randomly
   */
  private static class AllocateRandom extends AllocateStrategyBase {
    Random mRandm = new Random(System.currentTimeMillis());

    @Override
    public StorageDir getStorageDir(StorageDir[] storageDirs, long userId, long requestSizeBytes) {
      StorageDir availableDir = null;
      int i = mRandm.nextInt(storageDirs.length);
      for (StorageDir dir : storageDirs) {
        if (i == storageDirs.length) {
          i = 0;
        }
        if (dir.getAvailableBytes() >= requestSizeBytes) {
          availableDir = dir;
          if (availableDir.requestSpace(userId, requestSizeBytes)) {
            break;
          }
        }
        i ++;
      }
      return availableDir;
    }
  }

  /**
   * Allocate space on StorageDirs by round robin
   */
  private static class AllocateRR extends AllocateStrategyBase {
    int mDirIndex = 0;

    @Override
    public synchronized StorageDir getStorageDir(StorageDir[] storageDirs, long userId,
        long requestSizeBytes) {
      StorageDir availableDir = null;
      for (int j = 0; j < storageDirs.length; mDirIndex ++, j ++) {
        if (mDirIndex == storageDirs.length) {
          mDirIndex = 0;
        }
        if (storageDirs[mDirIndex].getAvailableBytes() >= requestSizeBytes) {
          availableDir = storageDirs[mDirIndex];
          mDirIndex ++;
          if (availableDir.requestSpace(userId, requestSizeBytes)) {
            break;
          }
        }
      }
      return availableDir;
    }
  }

  /**
   * Base class for AllocateStrategy, which provides basic function for AllocateStrategy
   */
  private abstract static class AllocateStrategyBase implements AllocateStrategy {
    @Override
    public boolean fitInPossible(StorageDir[] storageDirs, long requestSizeBytes) {
      boolean isPossible = false;
      for (StorageDir dir : storageDirs) {
        if (dir.getCapacityBytes() - dir.getLockedSizeBytes() >= requestSizeBytes) {
          isPossible = true;
          break;
        }
      }
      return isPossible;
    }
  }

  /**
   * Get AllocateStrategy based on configuration
   * 
   * @param allocateStrategy configuration of AllocateStrategy
   * @return AllocationStrategy generated
   */
  public static AllocateStrategy getAllocateStrategy(AllocateStrategyType strategyType) {
    switch (strategyType) {
      case MAX_FREE:
        return new AllocateMaxFree();
      case RANDOM:
        return new AllocateRandom();
      case ROUND_ROBIN:
        return new AllocateRR();
      default:
        return new AllocateMaxFree();
    }
  }

  private AllocateStrategies() {}
}
