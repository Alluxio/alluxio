package alluxio.worker.block.reviewer;

import alluxio.annotation.PublicApi;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockMetadataView;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.allocator.MaxFreeAllocator;
import alluxio.worker.block.meta.StorageDirView;
import com.google.common.base.Preconditions;

@PublicApi
public interface Reviewer {
  boolean reviewAllocation(StorageDirView dirView);

  /**
   * Factory for {@link Reviewer}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    // TODO(jiacheng): Is it possible we need a list of reviewers in the future, each checking one criteria?
    /**
     * Factory for {@link Reviewer}.
     *
     * @return the generated {@link Reviewer}, it will be a {@link ProbabilisticReviewer} by default
     */
    public static Reviewer create() {
      return CommonUtils.createNewClassInstance(
              ServerConfiguration.<Reviewer>getClass(PropertyKey.WORKER_REVIEWER_CLASS),
              new Class[] {}, new Object[] {});
    }
  }
}
