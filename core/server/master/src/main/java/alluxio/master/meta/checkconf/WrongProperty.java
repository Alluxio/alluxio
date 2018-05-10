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

package alluxio.master.meta.checkconf;

import java.util.List;
import java.util.Map;

/**
 * Records a property that is required to be consistent but is not within its scope.
 */
public class WrongProperty {
  /** The name of the property that has errors/warnings.*/
  private String mName;
  /**
   * Record the values and corresponding hostnames. */
  private Map<String, List<String>> mValues;

  /**
   * Creates a new instance of {@link WrongProperty}.
   */
  public WrongProperty() {}

  /**
   * @return the name of this property
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the values of this property
   */
  public Map<String, List<String>> getValues() {
    return mValues;
  }

  /**
   * @param name the property name
   * @return the wrong property
   */
  public WrongProperty setName(String name) {
    mName = name;
    return this;
  }

  /**
   * @param values the values to use
   * @return the wrong property
   */
  public WrongProperty setValues(Map<String, List<String>> values) {
    mValues = values;
    return this;
  }
}
