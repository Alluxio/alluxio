/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.allocator;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockMetadataView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.StorageDirView;

import com.google.common.base.Preconditions;

/**
 * Interface for the allocation policy of Alluxio managed data.
 */
@PublicApi
public interface Allocator {

  /**
   * Factory for {@link Allocator}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory for {@link Allocator}.
     *
     * @param view {@link BlockMetadataView} to pass to {@link Allocator}
     * @return the generated {@link Allocator}, it will be a {@link MaxFreeAllocator} by default
     */
    public static Allocator create(BlockMetadataView view) {
      BlockMetadataView metadataView = Preconditions.checkNotNull(view, "view");
      return CommonUtils.createNewClassInstance(
          ServerConfiguration.<Allocator>getClass(PropertyKey.WORKER_ALLOCATOR_CLASS),
          new Class[] {BlockMetadataView.class}, new Object[] {metadataView});
    }
  }

  /**
   * Allocates a block from the given block store location under a given view. The location can be a
   * specific location, or {@link BlockStoreLocation#anyTier()} or
   * {@link BlockStoreLocation#anyDirInTier(String)}.
   *
   * @param sessionId the id of session to apply for the block allocation
   * @param blockSize the size of block in bytes
   * @param location the location in block store
   * @param view of the block metadata
   * @return a {@link StorageDirView} in which to create the temp block meta if success, null
   *         otherwise
   */
  StorageDirView allocateBlockWithView(long sessionId, long blockSize, BlockStoreLocation location,
      BlockMetadataView view);
}
