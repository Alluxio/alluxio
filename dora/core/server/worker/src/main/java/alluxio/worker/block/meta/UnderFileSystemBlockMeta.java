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

import alluxio.proto.dataserver.Protocol;

/**
 * This class represents the metadata of a block that is in UFS. This class is immutable.
 */
public final class UnderFileSystemBlockMeta {
  private final long mSessionId;
  private final long mBlockId;
  private final String mUnderFileSystemPath;
  /** The offset in bytes of the first byte of the block in its corresponding UFS file. */
  private final long mOffset;
  /** The block size in bytes. */
  private final long mBlockSize;
  /** The id of the mount point. */
  private final long mMountId;
  /** Do not cache the block to the local Alluxio worker if set. */
  private final boolean mNoCache;
  private final String mUser;

  /**
   * Creates an instance of {@link UnderFileSystemBlockMeta}.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   * @param options the {@link Protocol.OpenUfsBlUfsInputStreamCacheTestockOptions}
   */
  public UnderFileSystemBlockMeta(long sessionId, long blockId,
      Protocol.OpenUfsBlockOptions options) {
    mSessionId = sessionId;
    mBlockId = blockId;
    mUnderFileSystemPath = options.getUfsPath();
    mOffset = options.getOffsetInFile();
    mBlockSize = options.getBlockSize();
    mMountId = options.getMountId();
    mNoCache = options.getNoCache();
    mUser = options.getUser();
  }

  /**
   * @return the session ID
   */
  public long getSessionId() {
    return mSessionId;
  }

  /**
   * @return the block ID
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the UFS path
   */
  public String getUnderFileSystemPath() {
    return mUnderFileSystemPath;
  }

  /**
   * @return the offset of the block in the UFS file
   */
  public long getOffset() {
    return mOffset;
  }

  /**
   * @return the block size in bytes
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * @return the id of the mount of this file is mapped to
   */
  public long getMountId() {
    return mMountId;
  }

  /**
   * @return true if mNoCache is set
   */
  public boolean isNoCache() {
    return mNoCache;
  }

  /**
   * @return the user
   */
  public String getUser() {
    return mUser;
  }
}
