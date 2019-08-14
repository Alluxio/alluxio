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

package alluxio.master.file.meta;

import alluxio.exception.BlockInfoException;

import java.util.List;

/**
 * Forwarding wrapper around an inode file view.
 */
public class InodeFile extends Inode implements InodeFileView {
  private final InodeFileView mDelegate;

  /**
   * @param delegate the inode to delegate to
   */
  public InodeFile(InodeFileView delegate) {
    super(delegate);
    mDelegate = delegate;
  }

  @Override
  public long getPersistJobId() {
    return mDelegate.getPersistJobId();
  }

  @Override
  public long getShouldPersistTime() {
    return mDelegate.getShouldPersistTime();
  }

  @Override
  public int getReplicationDurable() {
    return mDelegate.getReplicationDurable();
  }

  @Override
  public int getReplicationMax() {
    return mDelegate.getReplicationMax();
  }

  @Override
  public int getReplicationMin() {
    return mDelegate.getReplicationMin();
  }

  @Override
  public String getTempUfsPath() {
    return mDelegate.getTempUfsPath();
  }

  @Override
  public List<Long> getBlockIds() {
    return mDelegate.getBlockIds();
  }

  @Override
  public long getBlockSizeBytes() {
    return mDelegate.getBlockSizeBytes();
  }

  @Override
  public long getLength() {
    return mDelegate.getLength();
  }

  @Override
  public long getBlockContainerId() {
    return mDelegate.getBlockContainerId();
  }

  @Override
  public long getBlockIdByIndex(int blockIndex) throws BlockInfoException {
    return mDelegate.getBlockIdByIndex(blockIndex);
  }

  @Override
  public boolean isCacheable() {
    return mDelegate.isCacheable();
  }

  @Override
  public boolean isCompleted() {
    return mDelegate.isCompleted();
  }
}
