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

package alluxio;

import java.util.function.Supplier;

/**
 * Supplier for a configuration property default.
 */
public class DefaultSupplier implements Supplier<Object> {
  private final Supplier<Object> mSupplier;
  private final String mDescription;

  /**
   * @param supplier the value
   * @param description a description of the default value
   */
  public DefaultSupplier(Supplier<Object> supplier, String description) {
    mSupplier = supplier;
    mDescription = description;
  }

  @Override
  public Object get() {
    return mSupplier.get();
  }

  /**
   * @return a description of how the default value is determined
   */
  public String getDescription() {
    return mDescription;
  }
}
