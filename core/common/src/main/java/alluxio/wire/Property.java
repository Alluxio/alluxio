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

import alluxio.conf.Source;
import alluxio.grpc.ConfigProperty;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Wire representation of a configuration property.
 */
@ThreadSafe
public final class Property {
  /** Property key name. */
  private final String mName;
  /** Property value. */
  private final String mValue;
  /**
   * Property source, should be one of the values of {@link alluxio.conf.Source}.
   */
  private final String mSource;

  /**
   * Creates a new property.
   *
   * @param name property name
   * @param value property value
   * @param source property source
   */
  public Property(String name, @Nullable String value, Source source) {
    Preconditions.checkNotNull(name, "name");
    Preconditions.checkNotNull(source, "source");
    mName = name;
    mValue = value;
    mSource = source.toString();
  }

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
    ConfigProperty.Builder builder = ConfigProperty.newBuilder();
    builder.setName(mName);
    if (mValue != null) {
      builder.setValue(mValue);
    }
    builder.setSource(mSource);
    return builder.build();
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
