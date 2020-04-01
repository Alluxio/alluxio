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

package alluxio.client.block.stream;

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.grpc.RequestType;
import alluxio.wire.WorkerNetAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a streaming API to write to a file in the under file system through an Alluxio
 * worker's data server.
 */
@NotThreadSafe
public class UnderFileSystemFileOutStream extends BlockOutStream {
  private static final int ID_UNUSED = -1;
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerClientPool.class);

  /**
   * Creates an instance of {@link UnderFileSystemFileOutStream} that writes to a UFS file.
   *
   * @param context the file system context
   * @param address the data server address
   * @param options the out stream options
   * @return the under file system output stream
   */
  public static UnderFileSystemFileOutStream create(FileSystemContext context,
      WorkerNetAddress address, OutStreamOptions options) throws IOException {
//    String workerAddress = address.getHost();
//    if (!workerAddress.startsWith("alluxio-worker")) {
//      String workerService = "alluxio-worker-" + workerAddress.replace(".", "-");
//      address.setHost(workerService);
//      LOG.warn("Replace {} with {}", workerAddress, workerService);
//    }
    return new UnderFileSystemFileOutStream(GrpcDataWriter.create(context, address,
        ID_UNUSED, Long.MAX_VALUE, RequestType.UFS_FILE, options), address);
  }

  /**
   * Constructs a new {@link UnderFileSystemFileOutStream} with only one {@link DataWriter}.
   *
   * @param dataWriter the data writer
   */
  protected UnderFileSystemFileOutStream(DataWriter dataWriter, WorkerNetAddress address) {
    super(dataWriter, Long.MAX_VALUE, address);
  }
}
