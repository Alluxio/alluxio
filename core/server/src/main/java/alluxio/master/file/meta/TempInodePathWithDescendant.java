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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a path of locked {@link Inode}, starting from the root.
 */
@ThreadSafe
public final class TempInodePathWithDescendant extends InodePath {
  private AlluxioURI mDescendantUri;
  private Inode<?> mDescendantInode;

  public TempInodePathWithDescendant(InodePath inodePath) {
    super(inodePath);
    mDescendantUri = new AlluxioURI(inodePath.mUri.toString());
    mDescendantInode = null;
  }

  public synchronized void setDescendant(Inode<?> descendantInode, AlluxioURI uri) {
    mDescendantInode = descendantInode;
    mDescendantUri = uri;
  }

  @Override
  public synchronized AlluxioURI getUri() {
    if (mDescendantInode == null) {
      return super.getUri();
    }
    return mDescendantUri;
  }

  @Override
  public synchronized Inode getInode() throws FileDoesNotExistException {
    if (mDescendantInode == null) {
      return super.getInode();
    }
    return mDescendantInode;
  }

  @Override
  public synchronized  InodeDirectory getParentInodeDirectory()
      throws InvalidPathException, FileDoesNotExistException {
    if (mDescendantInode == null) {
      return super.getParentInodeDirectory();
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized List<Inode<?>> getInodeList() {
    if (mDescendantInode == null) {
      return super.getInodeList();
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized boolean fullPathExists() {
    if (mDescendantInode == null) {
      return super.fullPathExists();
    }
    return true;
  }

  @Override
  public synchronized void close() {
    // nothing to close
  }
}
