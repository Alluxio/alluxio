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

import alluxio.collections.Pair;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a pair of {@link LockedInodePath}s. This is threadsafe, since the
 * elements cannot set once the pair is constructed.
 */
@ThreadSafe
public final class InodePathPair extends Pair<LockedInodePath, LockedInodePath>
    implements AutoCloseable {

  InodePathPair(LockedInodePath inodePath1, LockedInodePath inodePath2) {
    super(inodePath1, inodePath2);
  }

  @Override
  public void setFirst(LockedInodePath first) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSecond(LockedInodePath second) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void close() {
    getFirst().close();
    getSecond().close();
  }
}
