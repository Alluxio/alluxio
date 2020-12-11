package alluxio.worker.block.reviewer;

import alluxio.worker.block.BlockMetadataView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.StorageDirView;

import java.util.Iterator;

public class AllocationCoordinator {
  public Reviewer mReviewer;
  public Allocator mAllocator;

  private static AllocationCoordinator sInstance = null;

  // TODO(jiacheng): singleton
  private AllocationCoordinator(BlockMetadataView view) {
    mAllocator = Allocator.Factory.create(view);
    mReviewer = Reviewer.Factory.create();
  }

  public static AllocationCoordinator getInstance(BlockMetadataView view) {
    if (sInstance == null) {
      synchronized (AllocationCoordinator.class) {
        if (sInstance == null) {
          sInstance = new AllocationCoordinator(view);
        }
      }
    }
    return sInstance;
  }

  /**
   * This is currently only used by tests, where the singleton state needs to be reconfigured.
   * */
  public static synchronized void destroyInstance() {
    sInstance = null;
  }

  public StorageDirView allocateBlockWithView(long sessionId, long blockSize, BlockStoreLocation location,
                                       BlockMetadataView view, boolean skipReview) {
    if (skipReview) {
      return mAllocator.allocateBlockWithView(sessionId, blockSize, location, view, v -> true);
    }
    return mAllocator.allocateBlockWithView(sessionId, blockSize, location, view, mReviewer::acceptAllocation);
  }

  public Allocator getAllocator() {
    return mAllocator;
  }

  public Reviewer getReviewer() {
    return mReviewer;
  }
}
