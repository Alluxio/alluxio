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

import alluxio.conf.PropertyKey;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * An Alluxio configuration record.
 */
public final class ConfigRecord {
  private PropertyKey mKey;
  private String mSource;
  private Optional<String> mValue;

  /**
   * Creates a new instance of {@link ConfigRecord}.
   */
  public ConfigRecord() {}

  /**
   * Creates a new instance of {@link ConfigRecord}.
   *
   * @param key the property key
   * @param source the source of the value
   * @param value the property value
   */
  public ConfigRecord(PropertyKey key, String source, @Nullable String value) {
    mKey = key;
    mSource = source;
    mValue = Optional.ofNullable(value);
  }

  /**
   * @return the property key
   */
  public PropertyKey getKey() {
    return mKey;
  }

  /**
   * @return the source of this property
   */
  public String getSource() {
    return mSource;
  }

  /**
   * @return the value of this property
   */
  public Optional<String> getValue() {
    return mValue;
  }

  /**
   * @param key the property key
   * @return the configuration record
   */
  public ConfigRecord setKey(PropertyKey key) {
    mKey = key;
    return this;
  }

  /**
   * @param source the source to use
   * @return the configuration record
   */
  public ConfigRecord setSource(String source) {
    mSource = source;
    return this;
  }

  /**
   * @param value the value to use
   * @return the configuration record
   */
  public ConfigRecord setValue(@Nullable String value) {
    mValue = Optional.ofNullable(value);
    return this;
  }
}
