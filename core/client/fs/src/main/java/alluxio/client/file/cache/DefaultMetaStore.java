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

import alluxio.exception.PageNotFoundException;

import java.util.HashSet;
import java.util.Set;

/**
 * The default implementation of a metadata store for pages stored in cache.
 */
public class DefaultMetaStore implements MetaStore {
  private final Set<PageId> mPageMap = new HashSet<>();

  @Override
  public boolean hasPage(PageId pageId) {
    return mPageMap.contains(pageId);
  }

  @Override
  public boolean addPage(PageId pageId) {
    return mPageMap.add(pageId);
  }

  @Override
  public void removePage(PageId pageId) throws PageNotFoundException {
    if (!mPageMap.remove(pageId)) {
      throw new PageNotFoundException(String.format("Page %s could not be found", pageId));
    }
  }
}
