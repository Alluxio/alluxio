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

package alluxio.dora.worker.dora;

import alluxio.dora.worker.DataWorker;
import alluxio.dora.worker.SessionCleanable;
import alluxio.dora.worker.block.io.BlockReader;
import alluxio.dora.grpc.GetStatusPOptions;
import alluxio.dora.proto.dataserver.Protocol;
import alluxio.dora.underfs.UfsStatus;
import alluxio.dora.underfs.options.ListOptions;
import alluxio.dora.wire.FileInfo;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * A block worker in the Alluxio system.
 */
public interface DoraWorker extends DataWorker, SessionCleanable {
  /**
   * Gets the file information.
   *
   * @param fileId the file id
   * @param options the options for the GetStatusPRequest
   * @return the file info
   */
  FileInfo getFileInfo(String fileId, GetStatusPOptions options) throws IOException;

  /**
   * List status from Under File System.
   *
   * Please refer to UnderFileSystem.listStatus().
   *
   * @param path the path of a dir or file
   * @param options the option for listStatus()
   * @return An array with the statuses of the files and directories in the directory denoted by
   *         this abstract pathname. The array will be empty if the directory is empty. Returns
   *         {@code null} if this abstract pathname does not denote a directory.
   * @throws IOException
   */
  @Nullable
  UfsStatus[] listStatus(String path, ListOptions options) throws IOException;

  /**
   * Creates the file reader to read from Alluxio dora.
   * Owner of this block reader must close it or lock will leak.
   *
   * @param fileId the ID of the UFS file
   * @param offset the offset within the block
   * @param positionShort whether the operation is using positioned read to a small buffer size
   * @param options the options
   * @return a block reader to read data from
   * @throws IOException if it fails to get block reader
   */
  BlockReader createFileReader(String fileId, long offset,
                               boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException;
}
