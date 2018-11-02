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

package alluxio.master.file.options;

import alluxio.security.authorization.Mode;
import alluxio.wire.CommonOptions;

import java.util.Collections;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a directory.
 */
@NotThreadSafe
public final class CreatePathOptionsUtils {
  /**
   * Set default master options for creating a path.
   *
   * @param options the options object to update
   */
  public static void setDefaults(alluxio.file.options.CreatePathOptions options) {
    options.setCommonOptions(CommonOptions.defaults());
    options.setMountPoint(false);
    options.setOperationTimeMs(System.currentTimeMillis());
    options.setOwner("");
    options.setGroup("");
    options.setMode(Mode.defaults());
    options.setAcl(Collections.emptyList());
    options.setPersisted(false);
    options.setRecursive(false);
    options.setMetadataLoad(false);
  }

  // Prevent instantiation
  private CreatePathOptionsUtils() {}
}
