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

import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.util.FileSystemOptions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The file system class to set default options for master.
 */
@ThreadSafe
public final class FileSystemMasterOptions {

  /**
   * @return Master side defaults for {@link CompleteFilePOptions}
   */
  public static CompleteFilePOptions completeFileDefaults() {
    return CompleteFilePOptions.newBuilder()
        .setCommonOptions(FileSystemOptions.commonDefaults(ServerConfiguration.global()))
        .setUfsLength(0)
        .build();
  }
}
