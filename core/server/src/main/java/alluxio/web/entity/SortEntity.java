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

public class SortEntity {
  public static final String SORT_ASC = "asc";
  public static final String SORT_DESC = "desc";
  private String field;
  private String direction;

  public String getField() {
    return field;
  }

  public String getDirection() {
    return direction;
  }

  public void setField(String field) {
    this.field = field;
  }

  public void setDirection(String direction) {
    this.direction = direction;
  }
}
