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

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.TtlAction;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Common util methods for executing commands.
 */
@ThreadSafe
@PublicApi
public final class FileSystemCommandUtils {

  private FileSystemCommandUtils() {} // prevent instantiation

  /**
   * Sets a new TTL value or unsets an existing TTL value for file at path.
   *
   * @param fs the file system for Alluxio
   * @param path the file path
   * @param ttlMs the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted, irrespective of
   *        whether the file is pinned; {@link Constants#NO_TTL} means to unset the TTL value
   * @param ttlAction Action to perform on Ttl expiry
   */
  public static void setTtl(FileSystem fs, AlluxioURI path, long ttlMs,
      TtlAction ttlAction) throws AlluxioException, IOException {
    SetAttributePOptions options = SetAttributePOptions.newBuilder().setRecursive(true)
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(ttlMs).setTtlAction(ttlAction).build())
        .build();
    fs.setAttribute(path, options);
  }

  /**
   * Sets pin state for the input path.
   *
   * @param fs The {@link FileSystem} client
   * @param path The {@link AlluxioURI} path as the input of the command
   * @param pinned the state to be set
   * @param mediumTypes a list of medium types to pin to
   */
  public static void setPinned(FileSystem fs, AlluxioURI path, boolean pinned,
      List<String> mediumTypes)
      throws AlluxioException, IOException {
    SetAttributePOptions options = SetAttributePOptions.newBuilder().setPinned(pinned)
        .addAllPinnedMedia(mediumTypes)
        .build();
    fs.setAttribute(path, options);
  }
}
