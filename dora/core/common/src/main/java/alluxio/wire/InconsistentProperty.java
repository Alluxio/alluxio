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

package alluxio.wire;

import alluxio.grpc.InconsistentPropertyValues;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * Records a property that is required or recommended to be consistent but is not within its scope.
 *
 * For example, the ConsistencyCheckLevel of key A is ENFORCE and its Scope is MASTER,
 * so this property is required to be consistent in all master nodes.
 * The ConsistencyCheckLevel of key B is WARN and its Scope is SERVER,
 * so this property is recommended to be consistent in all master and worker nodes.
 */
public final class InconsistentProperty {
  /** The name of the property that has errors/warnings. */
  private String mName = "";
  /** Record the values and corresponding hostnames. */
  private Map<Optional<String>, List<String>> mValues = new HashMap<>();

  /**
   * Creates a new instance of {@link InconsistentProperty}.
   */
  public InconsistentProperty() {}

  /**
   * Creates a new instance of {@link InconsistentProperty} from proto representation.
   *
   * @param inconsistentProperty the proto inconsistent property
   */
  protected InconsistentProperty(alluxio.grpc.InconsistentProperty inconsistentProperty) {
    mName = inconsistentProperty.getName();
    mValues = new HashMap<>(inconsistentProperty.getValuesCount());
    for (Map.Entry<String, InconsistentPropertyValues> entry : inconsistentProperty.getValuesMap()
        .entrySet()) {
      if (entry.getKey().equals(OPTIONAL_STRING_VAL)) {
        mValues.put(Optional.empty(), entry.getValue().getValuesList());
      } else {
        mValues.put(Optional.of(entry.getKey()), entry.getValue().getValuesList());
      }
    }
  }

  /**
   * @return the name of this property
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the values of this property
   */
  public Map<Optional<String>, List<String>> getValues() {
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
  public InconsistentProperty setValues(Map<Optional<String>, List<String>> values) {
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
    return MoreObjects.toStringHelper(this)
        .add("key", mName)
        .add("values", formatValues(mValues))
        .toString();
  }

  /**
   * @return the formatted string of mValues without Optional keyword
   */
  private static String formatValues(Map<Optional<String>, List<String>> values) {
    StringJoiner joiner = new StringJoiner(", ");
    for (Map.Entry<Optional<String>, List<String>> entry : values.entrySet()) {
      joiner.add(String.format("%s (%s)",
          entry.getKey().orElse("no value set"),
          String.join(", ", entry.getValue())));
    }
    return joiner.toString();
  }

  private static final String OPTIONAL_STRING_VAL = "__Optional__";

  /**
   * @return an inconsistent property of proto construct
   */
  public alluxio.grpc.InconsistentProperty toProto() {
    Map<String, InconsistentPropertyValues> inconsistentPropsMap = new HashMap<>();
    for (Map.Entry<Optional<String>, List<String>> entry : mValues.entrySet()) {
      String pKey = OPTIONAL_STRING_VAL;
      final Optional<String> key = entry.getKey();
      if (key.isPresent() && !key.get().isEmpty()) {
        pKey = key.get();
      }
      inconsistentPropsMap.put(pKey,
          InconsistentPropertyValues.newBuilder().addAllValues(entry.getValue()).build());
    }
    return alluxio.grpc.InconsistentProperty.newBuilder().setName(mName)
        .putAllValues(inconsistentPropsMap).build();
  }

  /**
   * Creates a new instance of {@link InconsistentProperty} from proto representation.
   *
   * @param inconsistentProperty the proto representation of an inconsistent property
   * @return the instance
   */
  public static InconsistentProperty fromProto(
          alluxio.grpc.InconsistentProperty inconsistentProperty) {
    return new InconsistentProperty(inconsistentProperty);
  }
}
