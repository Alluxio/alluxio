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

import java.util.ArrayList;
import java.util.List;

/**
 * The web page grid pagination options entity.
 */
public class PaginationOptionsEntity {
  private int mPageNumber;
  private int mPageSize;
  private List<SortEntity> mSorters = new ArrayList<>();
  private List<FilterEntity> mFilters = new ArrayList<>();

  /**
   * Creates page pagination options entity.
   */
  public PaginationOptionsEntity(){
  }

  /**
   * @return the page number
   */
  public int getPageNumber() {
    return mPageNumber;
  }

  /**
   * Sets the page number.
   *
   * @param pageNumber the page number
   */
  public void setPageNumber(int pageNumber) {
    this.mPageNumber = pageNumber;
  }

  /**
   * @return the page size
   */
  public int getPageSize() {
    return mPageSize;
  }

  /**
   * Sets the page size.
   *
   * @param pageSize the page size
   */
  public void setPageSize(int pageSize) {
    this.mPageSize = pageSize;
  }

  /**
   * @return the filters
   */
  public List<FilterEntity> getFilters() {
    return mFilters;
  }

  /**
   * Sets the page filters.
   *
   * @param filters the page filters
   */
  public void setFilters(List<FilterEntity> filters) {
    this.mFilters = filters;
  }

  /**
   * @return the sorters
   */
  public List<SortEntity> getSorters() {
    return mSorters;
  }

  /**
   * Sets the page sorters.
   *
   * @param sorters the page sorters
   */
  public void setSorters(List<SortEntity> sorters) {
    this.mSorters = sorters;
  }
}
