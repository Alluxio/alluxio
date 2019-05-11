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

import alluxio.grpc.ConfigProperty;

/**
 * Wire representation of a configuration property.
 */
public final class Property {
  /** Property key name. */
  private final String mName;
  /** Property value. */
  private final String mValue;
  /**
   * Property source, should be one of the values of {@link alluxio.conf.Source}.
   */
  private final String mSource;

  private Property(ConfigProperty property) {
    mName = property.getName();
    mValue = property.getValue();
    mSource = property.getSource();
  }

  /**
   * @param property the grpc representation of a property
   * @return the property parsed from the grpc representation
   */
  public static Property fromProto(ConfigProperty property) {
    return new Property(property);
  }

  /**
   * @return the grpc representation
   */
  public ConfigProperty toProto() {
    return ConfigProperty.newBuilder().setName(mName).setValue(mValue).setSource(mSource).build();
  }

  /**
   * @return the property key name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the property value
   */
  public String getValue() {
    return mValue;
  }

  /**
   * @return the property source
   */
  public String getSource() {
    return mSource;
  }
}
