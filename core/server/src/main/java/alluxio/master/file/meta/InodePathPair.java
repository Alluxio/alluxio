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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a path of locked {@link Inode}, starting from the root.
 */
@ThreadSafe
public class InodePathPair implements AutoCloseable {
  protected final InodePath mInodePath1;
  protected final InodePath mInodePath2;

  InodePathPair(InodePath inodePath1, InodePath inodePath2) {
    mInodePath1 = inodePath1;
    mInodePath2 = inodePath2;
  }

  public synchronized InodePath getFirst() {
    return mInodePath1;
  }

  public synchronized InodePath getSecond() {
    return mInodePath2;
  }

  @Override
  public synchronized void close() {
    mInodePath1.close();
    mInodePath2.close();
  }
}
