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
 * Read-only interface for an inode directory.
 */
public interface InodeDirectoryView extends InodeView {
  /**
   * @return true if the inode is a mount point, false otherwise
   */
  boolean isMountPoint();

  /**
   * @return true if we have loaded all the direct children's metadata once
   */
  boolean isDirectChildrenLoaded();

  /**
   * @return the number of children contained in the directory
   */
  long getChildCount();
}
