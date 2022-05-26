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

package alluxio.master;

import alluxio.executor.ExecutorServiceBuilder;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.executor.RpcExecutorType;
import alluxio.executor.ThreadPoolExecutorQueueType;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test for {@link ExecutorServiceBuilder}.
 */
public class ExecutorServiceBuilderTest {
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    ServerConfiguration.reset();
  }

  @Test
  public void startZeroParallelism() {
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_TYPE, RpcExecutorType.FJP);
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_FJP_PARALLELISM, 0);
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(
        "Cannot start Alluxio master gRPC thread pool with "
            + "%s=%s! The parallelism must be greater than 0!",
        PropertyKey.MASTER_RPC_EXECUTOR_FJP_PARALLELISM, 0));
    ExecutorServiceBuilder.buildExecutorService(ExecutorServiceBuilder.RpcExecutorHost.MASTER);
  }

  @Test
  public void startNegativeParallelism() {
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_TYPE, RpcExecutorType.FJP);
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_FJP_PARALLELISM, -1);
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(
        "Cannot start Alluxio master gRPC thread pool with"
            + " %s=%s! The parallelism must be greater than 0!",
        PropertyKey.MASTER_RPC_EXECUTOR_FJP_PARALLELISM.toString(), -1));
    ExecutorServiceBuilder.buildExecutorService(ExecutorServiceBuilder.RpcExecutorHost.MASTER);
  }

  @Test
  public void startZeroKeepAliveTime() {
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE, "0");
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(
        "Cannot start Alluxio master gRPC thread pool with %s=%s. "
            + "The keepalive time must be greater than 0!",
        PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE.toString(), 0));
    ExecutorServiceBuilder.buildExecutorService(ExecutorServiceBuilder.RpcExecutorHost.MASTER);
  }

  @Test
  public void startNegativeKeepAliveTime() {
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE, "-1");
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(
        "Cannot start Alluxio master gRPC thread pool with %s=%s. "
            + "The keepalive time must be greater than 0!",
        PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE.toString(), -1));
    ExecutorServiceBuilder.buildExecutorService(ExecutorServiceBuilder.RpcExecutorHost.MASTER);
  }

  @Test
  public void createTpeExecutor() {
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_TYPE, RpcExecutorType.TPE);
    ExecutorServiceBuilder.buildExecutorService(ExecutorServiceBuilder.RpcExecutorHost.MASTER);
  }

  @Test
  public void createTpeExecutorWithCoreThreadsTimeout() {
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_TYPE, RpcExecutorType.TPE);
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_TPE_ALLOW_CORE_THREADS_TIMEOUT, true);
    ExecutorServiceBuilder.buildExecutorService(ExecutorServiceBuilder.RpcExecutorHost.MASTER);
  }

  @Test
  public void createTpeExecutorWithCustomQueues() {
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_TYPE, RpcExecutorType.TPE);
    for (ThreadPoolExecutorQueueType queueType:
        ThreadPoolExecutorQueueType.values()) {
      ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_TPE_QUEUE_TYPE, queueType);
      ExecutorServiceBuilder.buildExecutorService(ExecutorServiceBuilder.RpcExecutorHost.MASTER);
    }
  }
}
