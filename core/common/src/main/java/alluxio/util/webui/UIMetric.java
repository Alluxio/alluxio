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
 * The Ui metric.
 */
public class UIMetric {
  private long mValue;

  /**
   * Instantiates a new Ui metric.
   *
   * @param value the value
   */
  @JsonCreator
  public UIMetric(@JsonProperty("value") long value) {
    mValue = value;
  }

  /**
   * @param value the value
   */
  public void setValue(long value) {
    mValue = value;
  }

  /**
   * @return the value
   */
  public long getValue() {
    return mValue;
  }
}
