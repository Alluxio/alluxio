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
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.runtime.UnauthenticatedRuntimeException;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPartialPOptions;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.CheckAccessContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.wire.FileInfo;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Iterable for {@link FileInfo} objects. Generates the list of files from file system master.
 */
public class FileIterable implements Iterable<FileInfo> {
  private final FileSystemMaster mFileSystemMaster;
  private final String mPath;
  private final Optional<String> mUser;
  private final boolean mUsePartialListing;
  private final Predicate<FileInfo> mFilter;

  /**
   * Creates a new instance of {@link FileIterable}.
   *
   * @param fileSystemMaster file system master
   * @param path path to list
   * @param user user to list as
   * @param usePartialListing whether to use partial listing
   * @param filter filter to apply to the file infos
   */
  public FileIterable(FileSystemMaster fileSystemMaster, String path, Optional<String> user,
      boolean usePartialListing, Predicate<FileInfo> filter) {
    mFileSystemMaster = requireNonNull(fileSystemMaster, "fileSystemMaster is null");
    mPath = requireNonNull(path, "path is null");
    mUser = requireNonNull(user, "user is null");
    mUsePartialListing = usePartialListing;
    mFilter = filter;
  }

  /**
   * @return file iterator. generate new iterator each time
   */
  public FileIterator iterator() {
    return new FileIterator();
  }

  /**
   * An iterator over {@link FileInfo} objects.
   */
  public class FileIterator implements Iterator<FileInfo> {
    private final ListStatusPOptions.Builder mListOptions = ListStatusPOptions
        .newBuilder()
        .setRecursive(true);
    private static final int PARTIAL_LISTING_BATCH_SIZE = 100;
    private String mStartAfter = "";
    private List<FileInfo> mFiles;
    private Iterator<FileInfo> mFileInfoIterator;

    private FileIterator() {
      checkAccess();
      if (mUsePartialListing) {
        partialListFileInfos();
      }
      else {
        listFileInfos(ListStatusContext.create(mListOptions));
      }
    }

    private void checkAccess() {
      AuthenticatedClientUser.set(mUser.orElse(null));
      try {
        mFileSystemMaster.checkAccess(new AlluxioURI(mPath), CheckAccessContext.defaults());
      } catch (FileDoesNotExistException | InvalidPathException e) {
        throw new NotFoundRuntimeException(e);
      } catch (AccessControlException e) {
        throw new UnauthenticatedRuntimeException(e);
      } catch (IOException e) {
        throw AlluxioRuntimeException.from(e);
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
      ListStatusContext context = ListStatusContext.create(ListStatusPartialPOptions
          .newBuilder()
          .setOptions(mListOptions)
          .setBatchSize(PARTIAL_LISTING_BATCH_SIZE)
          .setStartAfter(mStartAfter));
      listFileInfos(context);
      if (mFiles.size() > 0) {
        mStartAfter = mFiles
            .get(mFiles.size() - 1)
            .getPath();
      }
    }

    private void listFileInfos(ListStatusContext context) {
      try {
        AuthenticatedClientUser.set(mUser.orElse(null));
        mFiles = mFileSystemMaster
            .listStatus(new AlluxioURI(mPath), context)
            .stream()
            .filter(mFilter)
            .collect(Collectors.toList());
        mFileInfoIterator = mFiles.iterator();
      } catch (FileDoesNotExistException | InvalidPathException e) {
        throw new NotFoundRuntimeException(e);
      } catch (AccessControlException e) {
        throw new UnauthenticatedRuntimeException(e);
      } catch (IOException e) {
        throw AlluxioRuntimeException.from(e);
      } finally {
        AuthenticatedClientUser.remove();
      }
    }
  }
}
