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

package alluxio.client.file;

import alluxio.Constants;
import alluxio.network.connection.ThriftClientPool;
import alluxio.thrift.FileSystemWorkerClientService;

import org.apache.thrift.protocol.TProtocol;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * Class for managing a pool of {@link FileSystemWorkerClientService.Client}.
 */
@ThreadSafe
final class FileSystemWorkerThriftClientPool
    extends ThriftClientPool<FileSystemWorkerClientService.Client> {
  public FileSystemWorkerThriftClientPool(Subject subject, InetSocketAddress address,
      int maxCapacity, long gcThresholdMs) {
    super(subject, Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_NAME,
        Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_VERSION, address, maxCapacity, gcThresholdMs);
  }

  @Override
  protected FileSystemWorkerClientService.Client createThriftClient(TProtocol protocol) {
    return new FileSystemWorkerClientService.Client(protocol);
  }

  @Override
  protected String getServiceNameForLogging() {
    return "FileSystemWorker";
  }
}
