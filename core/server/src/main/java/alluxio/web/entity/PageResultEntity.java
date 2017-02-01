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

package alluxio.web.entity;

import alluxio.web.UIFileInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * The web page grid result entity.
 */
public class PageResultEntity {
  private int mTotalCount = 0;
  private List<UIFileInfo> mPageData = new ArrayList<>();

  /**
   * Creates page result entity.
   */
  public PageResultEntity(){
  }

  /**
   * @return the page total count
   */
  public int getTotalCount() {
    return mTotalCount;
  }

  /**
   * Sets the page of filter.
   *
   * @param totalCount the page totalCount
   */
  public void setTotalCount(int totalCount) {
    mTotalCount = totalCount;
  }

  /**
   * @return the page data
   */
  public List<UIFileInfo> getPageData() {
    return mPageData;
  }

  /**
   * Sets page data to display.
   *
   * @param pageData the page data to display
   */
  public void setPageData(List<UIFileInfo> pageData) {
    mPageData = pageData;
  }
}
