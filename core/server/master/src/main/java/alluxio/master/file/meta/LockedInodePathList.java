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

import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a list of locked inodePaths.
 */
@ThreadSafe
public class LockedInodePathList implements AutoCloseable, Iterable<LockedInodePath> {
  private final List<LockedInodePath> mInodePathList;

  /**
   * Creates a new instance of {@link LockedInodePathList}.
   *
   * @param inodePathList the list to be closed
   */
  public LockedInodePathList(List<LockedInodePath> inodePathList) {
    mInodePathList = inodePathList;
  }

  /**
   * get the associated inodePathList.
   * @return the list of inodePaths
   */
  public List<LockedInodePath> getInodePathList() {
    return mInodePathList;
  }

  @Override
  public void close() {
    for (LockedInodePath lockedInodePath : mInodePathList) {
      lockedInodePath.close();
    }
  }

  @Override
  public Iterator<LockedInodePath> iterator() {
    return getInodePathList().iterator();
  }
}
