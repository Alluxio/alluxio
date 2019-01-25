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

package alluxio.conf;

/**
 * Options for getting configuration values.
 */
public final class ConfigurationValueOptions {
  private boolean mUseDisplayValue = false;
  private boolean mUseRawValue = false;

  /**
   * @return the default {@link ConfigurationValueOptions}
   */
  public static ConfigurationValueOptions defaults() {
    return new ConfigurationValueOptions();
  }

  private ConfigurationValueOptions() {
    // prevents instantiation
  }

  /**
   * @return whether to use display value
   */
  public boolean shouldUseDisplayValue() {
    return mUseDisplayValue;
  }

  /**
   * @return whether to use raw value
   */
  public boolean shouldUseRawValue() {
    return mUseRawValue;
  }

  /**
   * @param useRawValue whether to use raw value
   * @return the {@link ConfigurationValueOptions} instance
   */
  public ConfigurationValueOptions useRawValue(boolean useRawValue) {
    mUseRawValue = useRawValue;
    return this;
  }

  /**
   * @param useDisplayValue whether to use display value
   * @return the {@link ConfigurationValueOptions} instance
   */
  public ConfigurationValueOptions useDisplayValue(boolean useDisplayValue) {
    mUseDisplayValue = useDisplayValue;
    return this;
  }
}
