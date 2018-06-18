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

package alluxio.master.file;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.file.FileSystemMasterOptionsService;
import alluxio.file.options.CommonOptions;
import alluxio.file.options.GetStatusOptions;

/**
 * The file system class to set default options.
 */
public final class DefaultFileSystemMasterOptions implements FileSystemMasterOptionsService {

  public void setDefaults(CommonOptions options) {
    if (options != null) {
      options.setSyncIntervalMs(Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL));
    }
  }

  public void setDefaults(GetStatusOptions options) {}
}
