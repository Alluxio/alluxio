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

public class PageResultEntity {
  private int totalCount = 0;
  private List<UIFileInfo> pageData = new ArrayList<>();

  public int getTotalCount() {
    return totalCount;
  }

  public void setTotalCount(int totalCount) {
    this.totalCount = totalCount;
  }

  public List<UIFileInfo> getPageData() {
    return pageData;
  }

  public void setPageData(List<UIFileInfo> pageData) {
    this.pageData = pageData;
  }
}
