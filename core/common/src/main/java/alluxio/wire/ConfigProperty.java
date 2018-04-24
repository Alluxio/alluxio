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

import com.google.common.base.Objects;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The Alluxio runtime configuration information.
 */
@NotThreadSafe
public final class ConfigProperty implements Serializable {
  private static final long serialVersionUID = 1025798648274314086L;

  private String mName;
  private String mSource;
  private String mValue;

  /**
   * Creates a new instance of {@link ConfigProperty}.
   */
  public ConfigProperty() {}

  /**
   * Creates a new instance of {@link ConfigProperty} from a thrift representation.
   *
   * @param configProperty the thrift representation of Alluxio configuration property
   */
  private ConfigProperty(alluxio.thrift.ConfigProperty configProperty) {
    mName = configProperty.getName();
    mSource = configProperty.getSource();
    mValue = configProperty.getValue();
  }

  /**
   * @return the name of this configuration property
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the source of this configuration property
   */
  public String getSource() {
    return mSource;
  }

  /**
   * @return the value of this configuration property
   */
  public String getValue() {
    return mValue;
  }

  /**
   * @param name the configuration name to use
   * @return the configuration property
   */
  public ConfigProperty setName(String name) {
    mName = name;
    return this;
  }

  /**
   * @param source the configuration source to use
   * @return the configuration property
   */
  public ConfigProperty setSource(String source) {
    mSource = source;
    return this;
  }

  /**
   * @param value the configuration value to use
   * @return the configuration property
   */
  public ConfigProperty setValue(String value) {
    mValue = value;
    return this;
  }

  /**
   * @return thrift representation of the configuration property
   */
  public alluxio.thrift.ConfigProperty toThrift() {
    return new alluxio.thrift.ConfigProperty(mName, mSource, mValue);
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param configProperty the thrift representation of a configuration property
   * @return the wire type configuration property
   */
  public static ConfigProperty fromThrift(alluxio.thrift.ConfigProperty configProperty) {
    return new ConfigProperty(configProperty);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ConfigProperty)) {
      return false;
    }
    ConfigProperty that = (ConfigProperty) o;
    return mName.equals(that.mName)
        && mSource.equals(that.mSource)
        && mValue.equals(that.mValue);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mName, mSource, mValue);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", mName)
        .add("source", mSource).add("value", mValue).toString();
  }
}
