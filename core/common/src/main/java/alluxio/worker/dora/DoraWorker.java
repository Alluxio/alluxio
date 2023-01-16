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

package alluxio.worker.dora;

import alluxio.proto.dataserver.Protocol;
import alluxio.wire.FileInfo;
import alluxio.worker.DataWorker;
import alluxio.worker.SessionCleanable;
import alluxio.worker.block.io.BlockReader;

import java.io.IOException;

/**
 * A block worker in the Alluxio system.
 */
public interface DoraWorker extends DataWorker, SessionCleanable {
  /**
   * Gets the file information.
   *
   * @param fileId the file id
   * @return the file info
   */
  FileInfo getFileInfo(String fileId) throws IOException;

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
