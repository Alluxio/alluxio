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
import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a path of locked {@link Inode}, starting from the root.
 */
@ThreadSafe
public final class InodePath implements Iterable<Inode<?>> {
  private final ArrayList<Inode<?>> mInodes;
  private final InodeLockGroup mLockGroup;

  InodePath(List<Inode<?>> inodes, InodeLockGroup lockGroup) {
    mInodes = new ArrayList<>(inodes);
    mLockGroup = lockGroup;
  }

  public synchronized Inode getInode() {
    if (mInodes.size() == 0) {
      return null;
    }
    return mInodes.get(mInodes.size() - 1);
  }

  public synchronized void unlock() {
    mLockGroup.unlock();
  }

  @Override
  public synchronized Iterator<Inode<?>> iterator() {
    return mInodes.iterator();
  }
}
