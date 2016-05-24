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
import alluxio.thrift.CancelUfsFileTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Options for canceling a UFS file.
 */
@PublicApi
@NotThreadSafe
public final class CancelUfsFileOptions {
  /**
   * @return the default {@link CancelUfsFileOptions}
   */
  public static CancelUfsFileOptions defaults() {
    return new CancelUfsFileOptions();
  }

  private CancelUfsFileOptions() {
    // No options currently
  }

  @Override
  public boolean equals(Object o) {
    return this == o || o instanceof CancelUfsFileOptions;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public CancelUfsFileTOptions toThrift() {
    return new CancelUfsFileTOptions();
  }
}
