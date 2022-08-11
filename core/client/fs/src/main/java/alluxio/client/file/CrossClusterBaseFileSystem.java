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

import alluxio.exception.AlluxioException;
import alluxio.grpc.PathInvalidation;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Extended implementation of {@link BaseFileSystem} that adds the call to subscribe to
 * invalidations for cross cluster sync.
 */
public class CrossClusterBaseFileSystem extends BaseFileSystem implements FileSystemCrossCluster {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterBaseFileSystem.class);

  /**
   * Constructs a new base cross cluster file system.
   *
   * @param fsContext file system context
   */
  public CrossClusterBaseFileSystem(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public void subscribeInvalidations(String localClusterId, String ufsPath,
                                     StreamObserver<PathInvalidation> stream)
      throws IOException, AlluxioException {
    rpc(client -> {
      ((RetryHandlingFileSystemMasterClient) client).subscribeInvalidations(localClusterId, ufsPath,
          stream);
      LOG.debug("Subscribe to cross cluster invalidations for path {}", ufsPath);
      return null;
    });
  }

  @Override
  public void updateCrossClusterConfigurationAddress(InetSocketAddress[] addresses)
      throws IOException, AlluxioException {
    rpc(client -> {
      ((RetryHandlingFileSystemMasterClient) client)
          .updateCrossClusterConfigurationAddress(addresses);
      LOG.debug("Update the cross cluster configuration address {}", (Object) addresses);
      return null;
    });
  }
}
