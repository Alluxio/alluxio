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

package alluxio.s3;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * An iterator for listing a given path, and its working mode is lazy loading.
 */
public class ListPrefixIterator implements Iterator<URIStatus> {

  private static final URIStatus[] EMPTY_ARRAY = new URIStatus[0];

  private final AlluxioURI mPath;
  private final ChildrenSupplier mChildrenSupplier;
  private final String mPrefix;
  private final Deque<URIStatus> mStack;
  private URIStatus[] mChildren;
  private int mIndex;

  /**
   *
   * @param path the path to list status
   * @param childrenSupplier the method is used to list status
   * @param prefix the prefix to filter
   */
  public ListPrefixIterator(AlluxioURI path, ChildrenSupplier childrenSupplier,
      @Nullable String prefix) {
    mPath = path;
    mChildrenSupplier = childrenSupplier;
    mPrefix = prefix == null ? path.getPath() : prefix;
    mStack = new ArrayDeque<>();
    checkPrefix();
    mChildren = listSortedChildren(mPath);
  }

  private void checkPrefix() {
    if (mPath.getPath().equals(mPrefix)) {
      return;
    }
    if (Objects.equals(new AlluxioURI(mPrefix).getParent(), mPath)) {
      return;
    }
    throw new ListPrefixException(new AlluxioException(
        "The prefix must be equal to the path or a subdirectory of the path."));
  }

  @Override
  public boolean hasNext() {
    return !mStack.isEmpty() || mIndex < mChildren.length;
  }

  @Override
  public URIStatus next() {
    if (mIndex < mChildren.length) {
      URIStatus status = mChildren[mIndex];
      if (status.isFolder()) {
        for (int i = mChildren.length - 1; i > mIndex; i--) {
          mStack.push(mChildren[i]);
        }
        mIndex = 0;
        mChildren = listSortedChildren(new AlluxioURI(status.getPath()));
      } else {
        mIndex++;
      }
      return status;
    }
    URIStatus status = mStack.pop();
    mIndex = 0;
    mChildren = EMPTY_ARRAY;
    if (status.isFolder()) {
      mChildren = listSortedChildren(new AlluxioURI(status.getPath()));
    }
    return status;
  }

  private URIStatus[] listSortedChildren(AlluxioURI parent) {
    try {
      return mChildrenSupplier.getChildren(parent).stream()
          .filter(status -> status.getPath().startsWith(mPrefix))
          .sorted(Comparator.comparing(URIStatus::getPath))
          .toArray(URIStatus[]::new);
    } catch (IOException | AlluxioException e) {
      throw new ListPrefixException(e);
    }
  }

  /**
   * A function interface for listing status.
   */
  public interface ChildrenSupplier {

    /**
     *
     * @param path
     * @return Children of given parent path
     * @throws IOException
     * @throws AlluxioException
     */
    List<URIStatus> getChildren(AlluxioURI path) throws IOException, AlluxioException;
  }

  /**
   * An RuntimeException to wrap Exception.
   */
  public static class ListPrefixException extends RuntimeException {

    /**
     *
     * @param cause
     */
    public ListPrefixException(Exception cause) {
      super(cause);
    }
  }
}
