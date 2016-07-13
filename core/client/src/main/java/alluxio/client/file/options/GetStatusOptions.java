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

import alluxio.annotation.PublicApi;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for getting the status of a path.
 */
@PublicApi
@NotThreadSafe
public final class GetStatusOptions {
  /**
   * @return the default {@link GetStatusOptions}
   */
  public static GetStatusOptions defaults() {
    return new GetStatusOptions();
  }

  private GetStatusOptions() {
    // No options currently
  }

  @Override
  public boolean equals(Object o) {
    return this == o || o instanceof GetStatusOptions;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).toString();
  }
}
