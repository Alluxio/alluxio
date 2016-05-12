/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.meta;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Manages the locks for a group of {@link Inode}.
 */
@ThreadSafe
public final class InodeLockGroup {
  private final List<Inode<?>> mReadLockedInodes;
  private final List<Inode<?>> mWriteLockedInodes;

  InodeLockGroup() {
    mReadLockedInodes = new ArrayList<>();
    mWriteLockedInodes = new ArrayList<>();
  }

  public synchronized void lockRead(Inode<?> inode) {
    inode.lockRead();
    mReadLockedInodes.add(inode);
  }

  public synchronized void lockWrite(Inode<?> inode) {
    inode.lockWrite();
    mWriteLockedInodes.add(inode);
  }

  public synchronized void unlock() {
    for (Inode<?> inode : mReadLockedInodes) {
      inode.unlockRead();
    }
    for (Inode<?> inode : mWriteLockedInodes) {
      inode.unlockWrite();
    }
    mReadLockedInodes.clear();
    mWriteLockedInodes.clear();
  }
}
