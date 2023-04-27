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
import alluxio.grpc.ListStatusPOptions;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.ListOptions;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Iterable for {@link FileInfo} objects. Generates the list of files from under file system.
 */
public class UfsFileIterable implements Iterable<FileInfo> {
  private final String mPath;
  private final Optional<String> mUser;
  private final boolean mUsePartialListing;
  private final Predicate<FileInfo> mFilter;
  private final UnderFileSystem mFs;

  /**
   * Creates a new instance of {@link FileIterable}.
   *
   * @param fs                under file system
   * @param path              path to list
   * @param user              user to list as
   * @param usePartialListing whether to use partial listing
   * @param filter            filter to apply to the file infos
   */
  public UfsFileIterable(UnderFileSystem fs, String path, Optional<String> user,
      boolean usePartialListing, Predicate<FileInfo> filter) {
    mFs = requireNonNull(fs, "fileSystem is null");
    mPath = requireNonNull(path, "path is null");
    mUser = requireNonNull(user, "user is null");
    mUsePartialListing = usePartialListing;
    mFilter = filter;
  }

  /**
   * @return file iterator. generate new iterator each time
   */
  public UfsFileIterable.FileIterator iterator() {
    return new FileIterator();
  }

  /**
   * An iterator over {@link FileInfo} objects.
   */
  public class FileIterator implements Iterator<FileInfo> {
    private final ListStatusPOptions.Builder mListOptions =
        ListStatusPOptions.newBuilder().setRecursive(true);
    private static final int PARTIAL_LISTING_BATCH_SIZE = 1000;
    private String mStartAfter = "";
    private List<FileInfo> mFiles;
    private Iterator<FileInfo> mFileInfoIterator;

    private FileIterator() {

      if (mUsePartialListing) {
        partialListFileInfos();
      }
      else {
        listFileInfos();
      }
    }

    @Override
    public boolean hasNext() {
      if (mUsePartialListing && !mFileInfoIterator.hasNext()) {
        partialListFileInfos();
      }
      return mFileInfoIterator.hasNext();
    }

    @Override
    public FileInfo next() {
      if (mUsePartialListing && !mFileInfoIterator.hasNext()) {
        partialListFileInfos();
      }
      return mFileInfoIterator.next();
    }

    private void partialListFileInfos() {
      if (!mStartAfter.isEmpty()) {
        mListOptions.setDisableAreDescendantsLoadedCheck(true);
      }
      listFileInfos();
      if (mFiles.size() > 0) {
        mStartAfter = mFiles.get(mFiles.size() - 1).getPath();
      }
    }

    private void listFileInfos() {
      try {
        AuthenticatedClientUser.set(mUser.orElse(null));
        Optional<UfsStatus[]> ufsStatuses = mFs.listStatuses(mPath, ListOptions.defaults());
        if (!ufsStatuses.isPresent() || ufsStatuses.get().length == 0) {
          mFiles = Collections.emptyList();
          mFileInfoIterator = Collections.emptyIterator();
        }
        else {
          mFiles = Arrays.stream(ufsStatuses.get()).map(this::transformUfsStatus).filter(mFilter)
                         .collect(Collectors.toList());
          mFileInfoIterator = mFiles.iterator();
        }
      } catch (IOException e) {
        throw AlluxioRuntimeException.from(e);
      } finally {
        AuthenticatedClientUser.remove();
      }
    }

    private FileInfo transformUfsStatus(UfsStatus ufsStatus) {
      AlluxioURI ufsUri = new AlluxioURI(PathUtils.concatPath(mPath,
          CommonUtils.stripPrefixIfPresent(ufsStatus.getName(), mPath)));
      FileInfo info = new FileInfo().setName(ufsUri.getName()).setPath(ufsUri.getPath())
                                    .setUfsPath(ufsUri.getPath())
                                    .setFolder(ufsStatus.isDirectory())
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
}
