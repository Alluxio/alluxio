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

package alluxio.fuse.options;

import com.beust.jcommander.IStringConverter;

/**
 * Base class for value converters.
 *
 * @param <T>
 */
public abstract class BaseValueConverter<T> implements IStringConverter<T> {
  private final String mOptionName;

  /**
   * Constructor.
   *
   * @param optionName the name of the option that this converter is used for
   */
  public BaseValueConverter(String optionName) {
    mOptionName = optionName;
  }

  /**
   * @return option name
   */
  public String getOptionName() {
    return mOptionName;
  }

  /**
   * Format an error string describing the reason why the conversion failed.
   *
   * @param problematicValue the user input that could not be converted to the target type
   * @param targetDesc description of the desired target type
   * @param cause cause of the failure
   * @return error string. Example: {@code "--timeout: couldn't convert "abc" to "time duration"
   *     because failed to parse "abc" as integer: NumberFormatException"}
   */
  protected String getErrorString(String problematicValue, String targetDesc,
      String cause) {
    return "\"" + getOptionName() + "\": couldn't convert \"" + problematicValue
        + "\" to " + targetDesc + " because " + cause;
  }
}
