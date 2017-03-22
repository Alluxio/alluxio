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

/**
 * The web page grid sort entity.
 */
public final class SortEntity {
  public static final String SORT_ASC = "asc";
  public static final String SORT_DESC = "desc";
  private String mField;
  private String mDirection;

  /**
   * Creates sort entity.
   */
  public SortEntity() {
  }

  /**
   * @return the sort field
   */
  public String getField() {
    return mField;
  }

  /**
   * @return the sort direction
   */
  public String getDirection() {
    return mDirection;
  }

  /**
   * Sets the sort field.
   *
   * @param field the sort field
   */
  public void setField(String field) {
    mField = field;
  }

  /**
   * Sets the sort direction.
   *
   * @param direction the sort direction
   */
  public void setDirection(String direction) {
    mDirection = direction;
  }
}
