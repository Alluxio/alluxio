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

package alluxio.worker.modules;

import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.resource.PooledResource;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import alluxio.wire.WorkerIdentity;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Provider that provides worker identity by registering with the master.
 */
public class LegacyWorkerIdentityProvider implements Provider<WorkerIdentity> {
  private static final Logger LOG = LoggerFactory.getLogger(LegacyWorkerIdentityProvider.class);

  private final BlockMasterClientPool mBlockMasterClientPool;
  private final WorkerProcess mWorkerProcess;

  /**
   * Constructor.
   *
   * @param bmcPool
   * @param workerProcess
   */
  @Inject
  public LegacyWorkerIdentityProvider(BlockMasterClientPool bmcPool, WorkerProcess workerProcess) {
    mBlockMasterClientPool = bmcPool;
    mWorkerProcess = workerProcess;
  }

  @Override
  public WorkerIdentity get() {
    RetryPolicy retry = RetryUtils.defaultWorkerMasterClientRetry();
    while (true) {
      try (PooledResource<BlockMasterClient> bmc = mBlockMasterClientPool.acquireCloseable()) {
        long id = bmc.get().getId(mWorkerProcess.getAddress());
        LOG.debug("Obtained worker id {} from master", id);
        return WorkerIdentity.ParserV0.INSTANCE.fromLong(id);
      } catch (IOException ioe) {
        if (!retry.attempt()) {
          throw AlluxioRuntimeException.from(ioe);
        }
      }
    }
  }
}
