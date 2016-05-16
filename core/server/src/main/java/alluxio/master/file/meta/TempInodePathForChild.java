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

import alluxio.exception.InvalidPathException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a temporary {@link InodePath}. This {@link InodePath} will not unlock the
 * inodes on close.
 */
@ThreadSafe
public final class TempInodePathForChild extends ExtensibleInodePath {

  /**
   * Constructs a temporary {@link InodePath} from an existing {@link InodePath}.
   *
   * @param inodePath the {@link InodePath} to create the temporary path from
   * @param childComponent the child component
   * @throws InvalidPathException if the path is invalid
   */
  public TempInodePathForChild(InodePath inodePath, String childComponent)
      throws InvalidPathException {
    super(inodePath.mUri.join(childComponent), inodePath.mInodes, inodePath.mLockGroup);
  }

  @Override
  public synchronized void close() {
    // nothing to close
  }
}
