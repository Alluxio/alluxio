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

package alluxio.util.webui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The Ui triple.
 */
public class UITriple implements Comparable<UITriple> {
  private String mLeft;
  private String mMiddle;
  private String mRight;

  /**
   * Instantiates a new Ui metric.
   *
   * @param left the left
   * @param middle the middle
   * @param right the right
   */
  @JsonCreator
  public UITriple(@JsonProperty("left") String left, @JsonProperty("middle") String middle,
      @JsonProperty("right") String right) {
    mLeft = left;
    mMiddle = middle;
    mRight = right;
  }

  /**
   * @return the left
   */
  public String getLeft() {
    return mLeft;
  }

  /**
   * @return the middle
   */
  public String getMiddle() {
    return mMiddle;
  }

  /**
   * @return the right
   */
  public String getRight() {
    return mRight;
  }

  /**
   * @param left the left
   */
  public void setLeft(String left) {
    mLeft = left;
  }

  /**
   * @param middle the middle
   */
  public void setMiddle(String middle) {
    mMiddle = middle;
  }

  /**
   * @param right the right
   */
  public void setRight(String right) {
    mRight = right;
  }

  @Override
  public int compareTo(UITriple o) {
    return this.mLeft.compareTo(o.mLeft);
  }
}
