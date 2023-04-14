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

package alluxio.master.metastore;

import alluxio.master.file.meta.InodeIterationResult;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Iterator over inodes that allows to skip a directory when iterating.
 */
public interface SkippableInodeIterator
    extends Iterator<InodeIterationResult>, Closeable {
  /**
   * Skip the children of the current inode during the iteration.
   */
  default void skipChildrenOfTheCurrent() {
    throw new UnsupportedOperationException("Operation not supported");
  }
}
