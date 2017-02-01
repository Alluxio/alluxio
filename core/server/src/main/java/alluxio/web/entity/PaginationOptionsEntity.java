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

public class PaginationOptionsEntity {
  private int pageNumber;
  private int pageSize;
  private List<SortEntity> sorters = new ArrayList<>();
  private List<FilterEntity> filters = new ArrayList<>();

  public int getPageNumber() {
    return pageNumber;
  }

  public void setPageNumber(int pageNumber) {
    this.pageNumber = pageNumber;
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }

  public List<FilterEntity> getFilters() {
    return filters;
  }

  public void setFilters(List<FilterEntity> filters) {
    this.filters = filters;
  }

  public List<SortEntity> getSorters() {
    return sorters;
  }

  public void setSorters(List<SortEntity> sorters) {
    this.sorters = sorters;
  }
}
