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
 * Read-only interface for an inode file.
 */
public interface InodeFileView extends InodeView {

  /**
   * @return the job id of the job persisting this file
   */
  long getPersistJobId();

  /**
   * @return the time that this file should start persisting
   */
  long getShouldPersistTime();

  /**
   * @return the durable number of block replication
   */
  int getReplicationDurable();

  /**
   * @return the maximum number of block replication
   */
  int getReplicationMax();

  /**
   * @return the minimum number of block replication
   */
  int getReplicationMin();

  /**
   * @return the temporary UFS path this file is persisted to
   */
  String getTempUfsPath();

  /**
   * @return a duplication of all the block ids of the file
   */
  List<Long> getBlockIds();

  /**
   * @return the block size in bytes
   */
  long getBlockSizeBytes();

  /**
   * @return the length of the file in bytes. This is not accurate before the file is closed
   */
  long getLength();

  /**
   * @return the block container ID for this inode file
   */
  long getBlockContainerId();

  /**
   * Gets the block id for a given index.
   *
   * @param blockIndex the index to get the block id for
   * @return the block id for the index
   * @throws BlockInfoException if the index of the block is out of range
   */
  long getBlockIdByIndex(int blockIndex) throws BlockInfoException;

  /**
   * @return true if the file is cacheable, false otherwise
   */
  boolean isCacheable();

  /**
   * @return true if the file is complete, false otherwise
   */
  boolean isCompleted();
}
