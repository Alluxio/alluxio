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

package alluxio.client.file.cache;

import alluxio.client.quota.CacheScope;

import com.google.common.base.MoreObjects;

import java.util.Objects;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A class identifies the information of a single cached page.
 */
@ThreadSafe
public class PageInfo {
  private final PageId mPageId;
  private final long mPageSize;
  private final FileInfo mFileInfo;

  /**
   * @param pageId page id
   * @param pageSize page size in bytes
   */
  public PageInfo(PageId pageId, long pageSize) {
    this(pageId, pageSize, new FileInfo(CacheScope.GLOBAL, 0));
  }

  /**
   * @param pageId page id
   * @param pageSize page size in bytes
   * @param fileInfo file info of this page
   */
  public PageInfo(PageId pageId, long pageSize, FileInfo fileInfo) {
    mPageId = pageId;
    mPageSize = pageSize;
    mFileInfo = fileInfo;
  }

  /**
   * @return page id
   */
  public PageId getPageId() {
    return mPageId;
  }

  /**
   * @return page size in bytes
   */
  public long getPageSize() {
    return mPageSize;
  }

  /**
   * @return file info of this page
   */
  public FileInfo getFileInfo() {
    return mFileInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PageInfo pageInfo = (PageInfo) o;
    return mPageSize == pageInfo.mPageSize && Objects.equals(mPageId, pageInfo.mPageId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mPageId, mPageSize);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("PageId", mPageId)
        .add("PageSize", mPageSize)
        .toString();
  }
}
