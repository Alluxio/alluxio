/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.options;

import alluxio.annotation.PublicApi;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method option for mounting a path.
 */
@PublicApi
@NotThreadSafe
public final class MountOptions {
  /**
   * @return the default {@link MountOptions}
   */
  @SuppressFBWarnings("ISC_INSTANTIATE_STATIC_CLASS")
  public static MountOptions defaults() {
    return new MountOptions();
  }

  private MountOptions() {
    // No options currently
  }
}
