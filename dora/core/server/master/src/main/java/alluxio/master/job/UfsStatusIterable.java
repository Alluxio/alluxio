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
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.ListOptions;

import com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Iterable for listing {@link UfsStatus} from {@link UnderFileSystem}.
 */
public class UfsStatusIterable implements Iterable<UfsStatus> {

  private final UnderFileSystem mUfs;
  private final String mPath;
  private final Optional<String> mUser;
  private final Predicate<UfsStatus> mFilter;
  private AlluxioURI mRootUri;

  /**
   * Creates a new instance of {@link UfsStatusIterable}.
   *
   * @param fs   under file system
   * @param path path to list
   * @param user user to list as
   * @param filter filter to apply to the listing
   */
  public UfsStatusIterable(UnderFileSystem fs, String path, Optional<String> user,
      Predicate<UfsStatus> filter) {
    mUfs = requireNonNull(fs, "fileSystem is null");
    mPath = requireNonNull(path, "path is null");
    mUser = requireNonNull(user, "user is null");
    mFilter = filter;
    mRootUri = new AlluxioURI(mPath);
  }

  @Override
  public Iterator<UfsStatus> iterator() {
    try {
      AuthenticatedClientUser.set(mUser.orElse(null));
      UfsStatus rootUfsStatus = mUfs.getStatus(mPath);
      if (rootUfsStatus != null && rootUfsStatus.isFile()) {
        if (rootUfsStatus.getUfsFullPath() == null) {
          rootUfsStatus.setUfsFullPath(mRootUri);
        }
        return Iterators.filter(Iterators.singletonIterator(rootUfsStatus), mFilter::test);
      }
      Iterator<UfsStatus> statuses =
          mUfs.listStatusIterable(mPath, ListOptions.defaults().setRecursive(true), null, 0);
      if (statuses == null) {
        throw new InternalRuntimeException("Get null when listing directory: " + mPath);
      }
      else {
        return Iterators.transform(Iterators.filter(statuses, mFilter::test), (it) -> {
          if (it.getUfsFullPath() == null) {
            it.setUfsFullPath(mRootUri.join(it.getName()));
          }
          return it;
        });
      }
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
  }
}
