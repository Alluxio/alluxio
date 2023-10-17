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

package alluxio.underfs;

import alluxio.collections.Pair;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

/**
 * This base under file system status iterator is for listing files iteratively.
 */
public class UfsFileStatusIterator implements Iterator<UfsStatus> {

  private static final Logger LOG = LoggerFactory.getLogger(UfsFileStatusIterator.class);

  private final UnderFileSystem mUfs;

  private final String mPath;

  /**
   * Each element is a pair of (full path, UfsStatus).
   */
  private LinkedList<Pair<String, UfsStatus>> mPathsToProcess = new LinkedList<>();

  /**
   * This base under file system status iterator is for listing files iteratively.
   * @param ufs the under file system
   * @param path the path for listing files
   */
  public UfsFileStatusIterator(UnderFileSystem ufs, String path) {
    mUfs = ufs;
    mPath = path;
    initQueue(path);
  }

  private void initQueue(String path) {
    try {
      UfsStatus[] statuses = mUfs.listStatus(path);
      if (statuses != null) {
        for (UfsStatus status : statuses) {
          mPathsToProcess.addFirst(
              new Pair<>(PathUtils.concatPath(path, status.getName()), status));
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to list files when calling listStatus", e);
    }
  }

  @Override
  public boolean hasNext() {
    return !mPathsToProcess.isEmpty();
  }

  @Override
  public UfsStatus next() {
    if (mPathsToProcess.isEmpty()) {
      throw new NoSuchElementException();
    }
    final Pair<String, UfsStatus> pathToProcessPair = mPathsToProcess.remove();
    final String pathToProcess = pathToProcessPair.getFirst();
    UfsStatus pathStatus = pathToProcessPair.getSecond();
    if (pathStatus.isFile()) {
      return pathStatus;
    }

    // The path is a directory, add all of its subpaths
    try {
      UfsStatus[] children = mUfs.listStatus(pathToProcess);
      if (children != null) {
        for (UfsStatus child : children) {
          mPathsToProcess.addFirst(
              new Pair<>(PathUtils.concatPath(pathToProcess, child.getName()), child));
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to list files when calling listStatus", e);
      throw new RuntimeException(e);
    }
    return next();
  }
}
