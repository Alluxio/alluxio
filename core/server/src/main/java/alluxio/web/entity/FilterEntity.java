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

public class FilterEntity {
  private String field;
  private String term;

  public void setField(String field) {
    this.field = field;
  }

  public void setTerm(String term) {
    this.term = term;
  }

  public String getField() {
    return field;
  }

  public String getTerm() {
    return term;
  }
}
