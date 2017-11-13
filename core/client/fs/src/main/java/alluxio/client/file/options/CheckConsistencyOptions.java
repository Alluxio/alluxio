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

package alluxio.client.file.options;

import alluxio.thrift.CheckConsistencyTOptions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Options for checking the consistency of an Alluxio subtree.
 */
@NotThreadSafe
public final class CheckConsistencyOptions extends CommonOptions<CheckConsistencyOptions> {
  /**
   * @return the default {@link CheckConsistencyOptions}
   */
  public static CheckConsistencyOptions defaults() {
    return new CheckConsistencyOptions();
  }

  private CheckConsistencyOptions() {
    // No options currently
  }

  @Override
  public CheckConsistencyOptions getThis() {
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CheckConsistencyOptions)) {
      return false;
    }
    if (!(super.equals(o))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return toStringHelper().toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public CheckConsistencyTOptions toThrift() {
    CheckConsistencyTOptions options = new CheckConsistencyTOptions();
    options.setCommonOptions(commonThrift());
    return options;
  }
}
