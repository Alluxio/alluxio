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
 * The web page grid filter entity.
 */
public class FilterEntity {
  private String mField;
  private String mTerm;

  /**
   * create filter entity.
   */
  public FilterEntity(){
  }

  /**
   * Sets the field of filter.
   *
   * @param field the field of filter
   */
  public void setField(String field) {
    mField = field;
  }

  /**
   * Sets the term to filter.
   *
   * @param term the term of filter
   */
  public void setTerm(String term) {
    mTerm = term;
  }

  /**
   * @return the field
   */
  public String getField() {
    return mField;
  }

  /**
   * @return the term
   */
  public String getTerm() {
    return mTerm;
  }
}
