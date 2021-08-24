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

package alluxio.stress.cli;

import alluxio.stress.rpc.RpcBenchParameters;
import alluxio.stress.rpc.RpcTaskResult;
import alluxio.util.executor.ExecutorServiceFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A benchmarking tool to simulate stress on RPCs.
 *
 * @param <T> the child {@link RpcBenchParameters} type
 */
public abstract class RpcBench<T extends RpcBenchParameters> extends Benchmark<RpcTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(RpcBench.class);
  protected ExecutorService mPool = null;

  /**
   * Defines how each thread runs RPCs.
   *
   * @return the results of RPC runs
   */
  public abstract RpcTaskResult runRPC() throws Exception;

  /**
   * Returns the parameters.
   *
   * @return the parameters for the benchmark class
   */
  public abstract T getParameters();

  /**
   * If the thread pool is not yet initialized, creates the pool.
   *
   * @return the thread pool
   */
  public ExecutorService getPool() {
    if (mPool == null) {
      mPool = ExecutorServiceFactories
          .fixedThreadPool("rpc-thread", getParameters().mConcurrency).create();
    }
    return mPool;
  }

  @Override
  public void cleanup() throws Exception {
    if (mPool != null) {
      LOG.debug("Terminating thread pool");
      mPool.shutdownNow();
      mPool.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  @Override
  public RpcTaskResult runLocal() throws Exception {
    RpcBenchParameters rpcBenchParameters = getParameters();
    LOG.info("Running locally with {} threads", rpcBenchParameters.mConcurrency);
    List<CompletableFuture<RpcTaskResult>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < rpcBenchParameters.mConcurrency; i++) {
        CompletableFuture<RpcTaskResult> future = CompletableFuture.supplyAsync(() -> {
          RpcTaskResult threadResult = new RpcTaskResult();
          threadResult.setBaseParameters(mBaseParameters);
          threadResult.setParameters(rpcBenchParameters);
          try {
            RpcTaskResult r = runRPC();
            threadResult.merge(r);
            return threadResult;
          } catch (Exception e) {
            LOG.error("Failed to execute RPC", e);
            threadResult.addError(e.getMessage());
            return threadResult;
          }
        }, getPool());
        futures.add(future);
      }
      LOG.info("{} jobs submitted", futures.size());

      // Collect the result
      RpcTaskResult merged = futures.stream()
          .map(CompletableFuture::join)
          .reduce(new RpcTaskResult(mBaseParameters, rpcBenchParameters),
              (sum, one) -> {
                sum.merge(one);
                return sum;
              });
      return merged;
    } catch (Exception e) {
      LOG.error("Failed to execute RPC in pool", e);
      RpcTaskResult result = new RpcTaskResult();
      result.setBaseParameters(mBaseParameters);
      result.setParameters(rpcBenchParameters);
      result.addError(e.getMessage());
      return result;
    }
  }
}
