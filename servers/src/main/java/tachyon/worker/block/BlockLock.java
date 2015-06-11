/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A ReadWrite Lock to guard one block. There should be only one lock per block.
 */
public class BlockLock implements ReadWriteLock {
  public enum BlockLockType {
    READ(0),  // A read lock
    WRITE(1);  // A write lock

    private final int mValue;

    BlockLockType(int value) {
      mValue = value;
    }

    public int getValue() {
      return mValue;
    }
  }

  private final ReentrantReadWriteLock mLock;
  /** The block Id this lock guards **/
  private final long mBlockId;


  public BlockLock(long blockId) {
    mBlockId = blockId;
    mLock = new ReentrantReadWriteLock();
  }

  public long getBlockId() {
    return mBlockId;
  }

  @Override
  public Lock readLock() {
    return mLock.readLock();
  }

  @Override
  public Lock writeLock() {
    return mLock.writeLock();
  }
}
