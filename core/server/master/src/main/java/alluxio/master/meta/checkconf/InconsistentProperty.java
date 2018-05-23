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

import com.google.common.base.Objects;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Records a property that is required or recommended to be consistent but is not within its scope.
 *
 * For example, the ConsistencyCheckLevel of key A is ENFORCE and its Scope is MASTER,
 * so this property is required to be consistent in all master nodes.
 * The ConsistencyCheckLevel of key B is WARN and its Scope is SERVER,
 * so this property is recommended to be consistent in all master and worker nodes.
 */
public class InconsistentProperty {
  /** The name of the property that has errors/warnings. */
  private String mName = "";
  /** Record the values and corresponding hostnames. */
  private Map<String, List<String>> mValues = new HashMap<>();

  /**
   * Creates a new instance of {@link InconsistentProperty}.
   */
  public InconsistentProperty() {}

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
   * @return the inconsistent property
   */
  public InconsistentProperty setName(String name) {
    mName = name;
    return this;
  }

  /**
   * @param values the values to use
   * @return the inconsistent property
   */
  public InconsistentProperty setValues(Map<String, List<String>> values) {
    mValues = values;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InconsistentProperty)) {
      return false;
    }
    InconsistentProperty that = (InconsistentProperty) o;
    return mName.equals(that.mName) && mValues.equals(that.mValues);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mName, mValues);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("key", mName)
        .add("values", mValues)
        .toString();
  }
}
