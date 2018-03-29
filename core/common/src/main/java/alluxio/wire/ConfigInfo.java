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
public final class ConfigInfo implements Serializable {
  private static final long serialVersionUID = 1025798648274314086L;

  private String mName;
  private String mValue;
  private String mSource;

  /**
   * Creates a new instance of {@link ConfigInfo}.
   */
  public ConfigInfo() {}

  /**
   * Creates a new instance of {@link ConfigInfo} from a thrift representation.
   *
   * @param configInfo the thrift representation of Alluxio configuration information
   */
  protected ConfigInfo(alluxio.thrift.ConfigInfo configInfo) {
    mName = configInfo.getName();
    mValue = configInfo.getValue();
    mSource = configInfo.getSource();
  }

  /**
   * @return the name of this configuration
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the value of this configuration
   */
  public String getValue() {
    return mValue;
  }

  /**
   * @return the source of this configuration
   */
  public String getSource() {
    return mSource;
  }

  /**
   * @param name the configuration name to use
   * @return the configuration information
   */
  public ConfigInfo setName(String name) {
    mName = name;
    return this;
  }

  /**
   * @param value the configuration value to use
   * @return the configuration information
   */
  public ConfigInfo setValue(String value) {
    mValue = value;
    return this;
  }

  /**
   * @param source the configuration source to use
   * @return the configuration information
   */
  public ConfigInfo setSource(String source) {
    mSource = source;
    return this;
  }

  /**
   * @return thrift representation of the configuration information
   */
  protected alluxio.thrift.ConfigInfo toThrift() {
    return new alluxio.thrift.ConfigInfo(mName, mValue, mSource);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ConfigInfo)) {
      return false;
    }
    ConfigInfo that = (ConfigInfo) o;
    return mName.equals(that.mName) && mValue.equals(that.mValue)
        && mSource.equals(that.mSource);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mName, mValue, mSource);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", mName)
        .add("value", mValue).add("source", mSource).toString();
  }
}
