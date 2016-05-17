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

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a pair of {@link LockedInodePath}s.
 */
@ThreadSafe
public final class InodePathPair implements AutoCloseable {
  private final LockedInodePath mInodePath1;
  private final LockedInodePath mInodePath2;

  InodePathPair(LockedInodePath inodePath1, LockedInodePath inodePath2) {
    mInodePath1 = inodePath1;
    mInodePath2 = inodePath2;
  }

  /**
   * @return the first of two {@link LockedInodePath}
   */
  public synchronized LockedInodePath getFirst() {
    return mInodePath1;
  }

  /**
   * @return the second of two {@link LockedInodePath}
   */
  public synchronized LockedInodePath getSecond() {
    return mInodePath2;
  }

  @Override
  public synchronized void close() {
    mInodePath1.close();
    mInodePath2.close();
  }
}
