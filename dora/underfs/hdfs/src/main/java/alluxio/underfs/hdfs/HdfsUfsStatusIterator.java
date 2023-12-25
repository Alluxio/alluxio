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

package alluxio.underfs.hdfs;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.util.UnderFileSystemUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

/**
 * HDFS under file system status iterator.
 */
public class HdfsUfsStatusIterator implements Iterator<UfsStatus> {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsUfsStatusIterator.class);

  private final FileSystem mFs;

  private final String mUfsSchemaUri;

  private final AlluxioURI mPathToList;

  /**
   * Each element is a pair of (full path, UfsStatus).
   */
  private LinkedList<Pair<String, UfsStatus>> mDirPathsToProcess = new LinkedList<>();

  private RemoteIterator<FileStatus> mHdfsRemoteIterator;

  /**
   * HDFS under file system status iterator.
   * @param pathToList the path for listing
   * @param fs the hdfs file system
   */
  public HdfsUfsStatusIterator(String pathToList, FileSystem fs) {
    mFs = fs;
    mUfsSchemaUri = mFs.getUri().toString();
    mPathToList = new AlluxioURI(pathToList);
    initQueue(pathToList);
  }

  private void initQueue(String path) {
    try {
      Path thePath = new Path(path);
      mHdfsRemoteIterator = mFs.listStatusIterator(thePath);
    } catch (IOException e) {
      LOG.error("Failed to list the path {}", path, e);
    }
  }

  @Override
  public boolean hasNext() {
    try {
      if (mHdfsRemoteIterator == null) {
        return false;
      }
      if (mHdfsRemoteIterator.hasNext()) {
        return true;
      }
      if (mDirPathsToProcess.isEmpty()) {
        return false;
      }
      while (!mDirPathsToProcess.isEmpty()) {
        Pair<String, UfsStatus> dir = mDirPathsToProcess.removeFirst();
        String path = dir.getFirst();
        mHdfsRemoteIterator = mFs.listStatusIterator(new Path(path));
        if (mHdfsRemoteIterator.hasNext()) {
          return true;
        }
      }
      return false;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public UfsStatus next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    try {
      FileStatus fileStatus = mHdfsRemoteIterator.next();
      UfsStatus ufsStatus;
      Path path = fileStatus.getPath();

      if (fileStatus.isDirectory()) {
        String relativePath = extractRelativePath(path.toUri().getPath());
        ufsStatus = new UfsDirectoryStatus(relativePath, fileStatus.getOwner(),
            fileStatus.getGroup(), fileStatus.getPermission().toShort(),
            fileStatus.getModificationTime());
        ufsStatus.setUfsFullPath(mPathToList.join(ufsStatus.getName()));
        mDirPathsToProcess.addLast(new Pair<>(path.toString(), ufsStatus));
      } else {
        String contentHash =
            UnderFileSystemUtils.approximateContentHash(
                fileStatus.getLen(), fileStatus.getModificationTime());
        String relativePath = extractRelativePath(path.toUri().getPath());
        ufsStatus = new UfsFileStatus(relativePath, contentHash, fileStatus.getLen(),
            fileStatus.getModificationTime(), fileStatus.getOwner(), fileStatus.getGroup(),
            fileStatus.getPermission().toShort(), fileStatus.getBlockSize());
        ufsStatus.setUfsFullPath(mPathToList.join(ufsStatus.getName()));
      }
      return ufsStatus;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String extractRelativePath(String fullPath) {
    String fullPathWithoutSchema = trimPathPrefix(mUfsSchemaUri, fullPath);
    String pathToListWithoutSchema = trimPathPrefix(mUfsSchemaUri, mPathToList.toString());
    if (!pathToListWithoutSchema.startsWith("/")) {
      pathToListWithoutSchema = "/" + pathToListWithoutSchema;
    }
    return trimPathPrefix(pathToListWithoutSchema, fullPathWithoutSchema);
  }

  private String trimPathPrefix(String prefix, String path) {
    if (path.startsWith(prefix)) {
      if (prefix.endsWith("/")) {
        return path.substring(prefix.length());
      } else {
        return path.substring(prefix.length() + 1);
      }
    } else {
      return path;
    }
  }
}
