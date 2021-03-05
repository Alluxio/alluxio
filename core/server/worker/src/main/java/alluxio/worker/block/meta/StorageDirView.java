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

package alluxio.worker.block.meta;

import alluxio.worker.block.BlockStoreLocation;

import com.google.common.base.Preconditions;

/**
 * This class is a wrapper of {@link StorageDir} to provide more limited access.
 */
public abstract class StorageDirView {

  /** The {@link StorageDir} this view is derived from. */
  final StorageDir mDir;
  /** The {@link StorageTierView} this view under. */
  final StorageTierView mTierView;

  /**
   * Creates a {@link StorageDirView} using the actual {@link StorageDir}.
   *
   * @param dir which the dirView is constructed from
   * @param tierView which the dirView is under
   */
  public StorageDirView(StorageDir dir, StorageTierView tierView) {
    mDir = Preconditions.checkNotNull(dir, "dir");
    mTierView = Preconditions.checkNotNull(tierView, "tierView");
  }

  /**
   * Gets available bytes for this dir.
   *
   * @return available bytes for this dir
   */
  public abstract long getAvailableBytes();

  /**
   * @return the amount of bytes reserved for internal management
   */
  public long getReservedBytes() {
    return mDir.getReservedBytes();
  }

  /**
   * Gets the index of this Dir.
   *
   * @return index of the dir
   */
  public int getDirViewIndex() {
    return mDir.getDirIndex();
  }

  /**
   * Gets capacity bytes for this dir.
   *
   * @return capacity bytes for this dir
   */
  public long getCapacityBytes() {
    return mDir.getCapacityBytes();
  }

  /**
   * Gets committed bytes for this dir. This includes all blocks, locked, pinned, committed etc.
   *
   * @return committed bytes for this dir
   */
  public long getCommittedBytes() {
    return mDir.getCommittedBytes();
  }

  /**
   * Creates a {@link TempBlockMeta} given sessionId, blockId, and initialBlockSize.
   *
   * @param sessionId of the owning session
   * @param blockId of the new block
   * @param initialBlockSize of the new block
   * @return a new {@link TempBlockMeta} under the underlying directory
   */
  public TempBlockMeta createTempBlockMeta(long sessionId, long blockId, long initialBlockSize) {
    return new DefaultTempBlockMeta(sessionId, blockId, initialBlockSize, mDir);
  }

  /**
   * @return parent tier view
   */
  public StorageTierView getParentTierView() {
    return mTierView;
  }

  /**
   * Get medium type of the dir view, which is derived from the dir.
   *
   * @return the medium type
   */
  public String getMediumType() {
    return mDir.getDirMedium();
  }

  /**
   * Creates a {@link BlockStoreLocation} for this directory view. Redirecting to
   * {@link StorageDir#toBlockStoreLocation}
   *
   * @return {@link BlockStoreLocation} created
   */
  public BlockStoreLocation toBlockStoreLocation() {
    return mDir.toBlockStoreLocation();
  }
}
