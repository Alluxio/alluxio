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

import java.util.Optional;

/**
 * Base converter for optional values.
 *
 * @param <T>
 */
public abstract class BaseOptionalValueConverter<T> extends BaseValueConverter<Optional<T>> {
  protected final IStringConverter<T> mDelegate;

  /**
   * Constructor.
   *
   * @param optionName the name of the option that this converter is used for
   * @param converter the converter to use when the value is present
   */
  public BaseOptionalValueConverter(String optionName, IStringConverter<T> converter) {
    super(optionName);
    mDelegate = converter;
  }

  @Override
  public Optional<T> convert(String value) {
    return Optional.empty();
  }

  protected abstract T convertOptional(String value);
}
