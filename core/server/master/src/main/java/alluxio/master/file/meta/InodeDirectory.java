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

/**
 * Forwarding wrapper around an inode directory view.
 */
public class InodeDirectory extends Inode implements InodeDirectoryView {
  private final InodeDirectoryView mDelegate;

  /**
   * @param delegate the inode to delegate to
   */
  public InodeDirectory(InodeDirectoryView delegate) {
    super(delegate);
    mDelegate = delegate;
  }

  @Override
  public boolean isMountPoint() {
    return mDelegate.isMountPoint();
  }

  @Override
  public boolean isDirectChildrenLoaded() {
    return mDelegate.isDirectChildrenLoaded();
  }

  @Override
  public long getChildCount() {
    return mDelegate.getChildCount();
  }
}
