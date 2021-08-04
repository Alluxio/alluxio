package alluxio.stress.cli;

import alluxio.stress.Parameters;
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
import java.util.stream.Collectors;

public abstract class RpcBench<T extends RpcBenchParameters> extends Benchmark<RpcTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(RpcBench.class);

  public abstract RpcTaskResult runRPC() throws Exception;

  public abstract T getParameters();

  @Override
  public RpcTaskResult runLocal() throws Exception {
    RpcBenchParameters rpcBenchParameters = getParameters();
    // TODO(jiacheng): refactor this and rename
    LOG.debug("Running locally with {} threads", rpcBenchParameters.mConcurrency);
    ExecutorService pool = null;
    List<CompletableFuture<RpcTaskResult>> futures = new ArrayList<>();
    try {
      pool = ExecutorServiceFactories.fixedThreadPool("rpc-thread", rpcBenchParameters.mConcurrency)
              .create();
      for (int i = 0; i < rpcBenchParameters.mConcurrency; i++) {
        CompletableFuture<RpcTaskResult> future = CompletableFuture.supplyAsync(() -> {
          RpcTaskResult threadResult = new RpcTaskResult();
          threadResult.setBaseParameters(mBaseParameters);
          threadResult.setParameters(rpcBenchParameters);
          try {
            RpcTaskResult r = runRPC();
            threadResult.setPoints(r.getPoints());
            threadResult.setErrors(r.getErrors());
            return threadResult;
          } catch (Exception e) {
            LOG.error("Failed to execute RPC", e);
            threadResult.addError(e.getMessage());
            return threadResult;
          }
        }, pool);
        futures.add(future);
      }
      LOG.info("{} jobs submitted", futures.size());

      // Collect the result
      CompletableFuture[] cfs = futures.toArray(new CompletableFuture[0]);
      List<RpcTaskResult> results = CompletableFuture.allOf(cfs)
              .thenApply(f -> futures.stream()
                      .map(CompletableFuture::join)
                      .collect(Collectors.toList())
              ).get();
      LOG.info("{} futures collected: {}", results.size(),
              results.size() > 0 ? results.get(0) : "[]");
      return RpcTaskResult.reduceList(results);
    } catch (Exception e) {
      LOG.error("Failed to execute RPC in pool", e);
      RpcTaskResult result = new RpcTaskResult();
      result.setBaseParameters(mBaseParameters);
      result.setParameters(rpcBenchParameters);
      result.addError(e.getMessage());
      return result;
    } finally {
      if (pool != null) {
        pool.shutdownNow();
        pool.awaitTermination(30, TimeUnit.SECONDS);
      }
    }
  }

}
