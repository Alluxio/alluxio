package alluxio.stress.cli;

import alluxio.ClientContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.master.MasterClientContext;
import alluxio.stress.CachingBlockMasterClient;
import alluxio.stress.rpc.GetPinnedFileIdsParameters;
import alluxio.stress.rpc.RegisterWorkerParameters;
import alluxio.stress.rpc.RpcTaskResult;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.worker.file.FileSystemMasterClient;
import com.beust.jcommander.ParametersDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class GetPinnedFileIdsBench extends Benchmark<RpcTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(GetPinnedFileIdsBench.class);

  @ParametersDelegate
  private GetPinnedFileIdsParameters mParameters = new GetPinnedFileIdsParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();

  @Override
  public RpcTaskResult runLocal() throws Exception {
    LOG.debug("Running locally with {} threads", mParameters.mConcurrency);
    ExecutorService pool = null;
    List<CompletableFuture<RpcTaskResult>> futures = new ArrayList<>();
    try {
      pool = ExecutorServiceFactories.fixedThreadPool("rpc-thread", mParameters.mConcurrency)
              .create();
      for (int i = 0; i < mParameters.mConcurrency; i++) {
        CompletableFuture<RpcTaskResult> future = CompletableFuture.supplyAsync(() -> {
          RpcTaskResult threadResult = new RpcTaskResult();
          threadResult.setBaseParameters(mBaseParameters);
          threadResult.setParameters(mParameters);
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
      result.setParameters(mParameters);
      result.addError(e.getMessage());
      return result;
    } finally {
      if (pool != null) {
        pool.shutdownNow();
        pool.awaitTermination(30, TimeUnit.SECONDS);
      }
    }

    return null;
  }

  @Override
  public void prepare() throws Exception {
    // TODO(bowen): you should be able to prepare something here
    //  for example we are getting the pinned file IDs, then those files should be
    //  prepared in Alluxio. You should find a way to generate those file IDs.
    //  The alluxio.CLIENT.file.FileSystemMasterClient has RPCs that can probably be
    //  used to fake those files?
    //  StressMasterBench is doing similar thing but calling from the FileSystem.createFile API.
    //  I think we can either call the alluxio.client.file.FileSystemMasterClient API or the
    //  FileSystem API.
  }

  private RpcTaskResult runRPC() throws Exception {
    FileSystemMasterClient client = new FileSystemMasterClient(MasterClientContext
            .newBuilder(ClientContext.create(mConf)).build());
    // TODO(bowen): keep calling client.getPinList() during the duration and collect results

    return null;
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new GetPinnedFileIdsBench());
  }
}
