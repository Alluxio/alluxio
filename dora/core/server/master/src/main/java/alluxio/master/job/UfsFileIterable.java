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

package alluxio.master.job;

import static java.util.Objects.requireNonNull;

import alluxio.AlluxioURI;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.FailedPreconditionRuntimeException;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.ListOptions;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Iterable for {@link FileInfo} objects. Generates the list of files from under file system.
 */
public class UfsFileIterable implements Iterable<FileInfo> {
  private final String mPath;
  private final Optional<String> mUser;

  private final Predicate<FileInfo> mFilter;
  private final UnderFileSystem mUfs;

  /**
   * Creates a new instance of {@link FileIterable}.
   *
   * @param fs     under file system
   * @param path   path to list
   * @param user   user to list as
   * @param filter filter to apply to the file infos
   */
  public UfsFileIterable(UnderFileSystem fs, String path, Optional<String> user,
      Predicate<FileInfo> filter) {
    mUfs = requireNonNull(fs, "fileSystem is null");
    mPath = requireNonNull(path, "path is null");
    mUser = requireNonNull(user, "user is null");
    mFilter = filter;
  }

  /**
   * @return file iterator. generate new iterator each time
   */
  @Override
  public Iterator<FileInfo> iterator() {
    try {
      AuthenticatedClientUser.set(mUser.orElse(null));
      UfsStatus rootUfsStatus = mUfs.getStatus(mPath);
      if (rootUfsStatus != null && rootUfsStatus.isFile()) {
        rootUfsStatus.setUfsFullPath(new AlluxioURI(mPath));
        return Iterators.filter(Iterators.singletonIterator(transformUfsStatus(rootUfsStatus)),
            mFilter::test);
      }
      Iterator<UfsStatus> statuses =
          mUfs.listStatusIterable(mPath, ListOptions.defaults().setRecursive(true), null, 0);
      if (statuses == null) {
        throw new FailedPreconditionRuntimeException("Get null when listing directory: " + mPath);
      }
      else {
        Iterator<FileInfo> infoIterator = Iterators.transform(statuses, this::transformUfsStatus);
        return Iterators.filter(infoIterator, mFilter::test);
      }
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
  }

  private FileInfo transformUfsStatus(UfsStatus ufsStatus) {
    AlluxioURI ufsUri = new AlluxioURI(
        PathUtils.concatPath(mPath, CommonUtils.stripPrefixIfPresent(ufsStatus.getName(), mPath)));
    FileInfo info = new FileInfo().setName(ufsUri.getName()).setPath(ufsUri.getPath())
                                  .setUfsPath(ufsUri.toString()).setFolder(ufsStatus.isDirectory())
                                  .setOwner(ufsStatus.getOwner()).setGroup(ufsStatus.getGroup())
                                  .setMode(ufsStatus.getMode()).setCompleted(true);
    if (ufsStatus.getLastModifiedTime() != null) {
      info.setLastModificationTimeMs(info.getLastModificationTimeMs());
    }
    if (ufsStatus.getXAttr() != null) {
      info.setXAttr(ufsStatus.getXAttr());
    }
    if (ufsStatus instanceof UfsFileStatus) {
      UfsFileStatus fileStatus = (UfsFileStatus) ufsStatus;
      info.setLength(fileStatus.getContentLength());
      info.setBlockSizeBytes(fileStatus.getBlockSize());
    }
    else {
      info.setLength(0);
    }
    return info;
  }
}

