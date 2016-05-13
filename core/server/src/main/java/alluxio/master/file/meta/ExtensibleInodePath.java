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

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a path of locked {@link Inode}, starting from the root.
 */
@ThreadSafe
public final class ExtensibleInodePath extends InodePath {
  ExtensibleInodePath(AlluxioURI uri, List<Inode<?>> inodes, InodeLockGroup lockGroup)
      throws InvalidPathException {
    super(uri, inodes, lockGroup);
  }

  String[] getPathComponents() {
    return mPathComponents;
  }

  List<Inode<?>> getInodes() {
    return mInodes;
  }

  InodeLockGroup getLockGroup() {
    return mLockGroup;
  }
}
