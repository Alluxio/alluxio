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

package alluxio.worker.netty;

import alluxio.Configuration;
import alluxio.worker.file.FileSystemWorker;

/**
 * This class handles filesystem data server requests.
 */
public class FileDataServerHandler {
  /**
   * Constructs a file data server handler for serving any ufs read/write requests.
   *
   * @param worker the file system worker
   * @param conf the configuration to use
   */
  public FileDataServerHandler(FileSystemWorker worker, Configuration conf) {

  }
}
