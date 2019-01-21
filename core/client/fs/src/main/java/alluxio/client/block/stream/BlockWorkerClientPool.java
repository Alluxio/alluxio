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

import alluxio.resource.DynamicResourcePool;
import alluxio.util.ThreadFactoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * Class for managing block worker clients. After obtaining a client with
 * {@link DynamicResourcePool#acquire()}, {@link DynamicResourcePool#release(Object)} must be called
 * when the thread is done using the client.
 */
@ThreadSafe
public final class BlockWorkerClientPool extends DynamicResourcePool<BlockWorkerClient> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerClientPool.class);
  private final Subject mSubject;
  private final SocketAddress mAddress;
  private static final int WORKER_CLIENT_POOL_GC_THREADPOOL_SIZE = 10;
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(WORKER_CLIENT_POOL_GC_THREADPOOL_SIZE,
          ThreadFactoryUtils.build("BlockWorkerClientPoolGcThreads-%d", true));
  private final long mGcThresholdMs;

  /**
   * Creates a new block master client pool.
   *
   * @param subject the parent subject
   * @param address address of the worker
   * @param maxCapacity the maximum capacity of the pool
   * @param gcThresholdMs when a client is older than this threshold and the pool's capacity
   *        is above the minimum capacity(1), it is closed and removed from the pool.
   */
  public BlockWorkerClientPool(Subject subject, SocketAddress address, int maxCapacity,
      long gcThresholdMs) {
    super(Options.defaultOptions().setMaxCapacity(maxCapacity).setGcExecutor(GC_EXECUTOR));
    mSubject = subject;
    mAddress = address;
    mGcThresholdMs = gcThresholdMs;
  }

  @Override
  protected void closeResource(BlockWorkerClient client) throws IOException {
    LOG.info("Block worker client for {} closed.", mAddress);
    client.close();
  }

  @Override
  protected BlockWorkerClient createNewResource() throws IOException {
    return BlockWorkerClient.Factory.create(mSubject, mAddress);
  }

  /**
   * Checks whether a client is healthy.
   *
   * @param client the client to check
   * @return true if the client is active (i.e. connected)
   */
  @Override
  protected boolean isHealthy(BlockWorkerClient client) {
    return !client.isShutdown();
  }

  @Override
  protected boolean shouldGc(ResourceInternal<BlockWorkerClient> clientResourceInternal) {
    return System.currentTimeMillis() - clientResourceInternal
        .getLastAccessTimeMs() > mGcThresholdMs;
  }
}
