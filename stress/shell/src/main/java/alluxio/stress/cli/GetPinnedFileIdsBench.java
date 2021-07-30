package alluxio.stress.cli;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileSystemContext;
import alluxio.collections.Pair;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.MasterClientContext;
import alluxio.resource.CloseableResource;
import alluxio.stress.CachingBlockMasterClient;
import alluxio.stress.PinListFileSystemMasterClient;
import alluxio.stress.rpc.GetPinnedFileIdsParameters;
import alluxio.stress.rpc.RegisterWorkerParameters;
import alluxio.stress.rpc.RpcTaskResult;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.worker.file.FileSystemMasterClient;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GetPinnedFileIdsBench extends Benchmark<RpcTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(GetPinnedFileIdsBench.class);

  @ParametersDelegate
  private GetPinnedFileIdsParameters mParameters = new GetPinnedFileIdsParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();
  private final FileSystemContext mFileSystemContext;
  private final AlluxioURI mBaseUri;

  {
    mFileSystemContext = FileSystemContext.create(mConf);
    mBaseUri = new AlluxioURI(mParameters.mBasePath);
  }

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
  }

  @Override
  public void prepare() throws Exception {
    try (CloseableResource<alluxio.client.file.FileSystemMasterClient> client =
             mFileSystemContext.acquireMasterClientResource()) {
      client.get().createDirectory(mBaseUri,
              CreateDirectoryPOptions.newBuilder().setAllowExists(true).build());
    }
    int fileNameLength = (int) Math.max(8, Math.log10(mParameters.mNumFiles));

    LOG.info("Generating {} test files", mParameters.mNumFiles);
    Stream.generate(() -> mBaseUri.join(CommonUtils.randomAlphaNumString(fileNameLength)))
        .limit(mParameters.mNumFiles)
        .parallel()
        .forEach((fileUri) -> {
            try (CloseableResource<alluxio.client.file.FileSystemMasterClient> client =
                     mFileSystemContext.acquireMasterClientResource()) {
              client.get().createFile(fileUri,
                  CreateFilePOptions
                      .newBuilder()
                      .setBlockSizeBytes(mConf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT))
                      .build());
              client.get().setAttribute(fileUri,
                  SetAttributePOptions
                      .newBuilder()
                      .setPinned(true)
                      .build());
            } catch (AlluxioStatusException e) {
              LOG.warn("Exception during file creation of {}", fileUri, e);
              System.exit(-1);
            }
        });
  }

  @Override
  public void cleanup() throws Exception {
    try (CloseableResource<alluxio.client.file.FileSystemMasterClient> client =
             mFileSystemContext.acquireMasterClientResource()) {
      client.get().delete(mBaseUri, DeletePOptions.newBuilder().setRecursive(true).build());
    }
  }

  private RpcTaskResult runRPC() throws Exception {
    PinListFileSystemMasterClient client = new PinListFileSystemMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(mConf)).build());
    RpcTaskResult result = new RpcTaskResult();
    result.setBaseParameters(mBaseParameters);
    result.setParameters(mParameters);

    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    Stopwatch durationStopwatch = Stopwatch.createStarted();
    Stopwatch pointStopwatch = Stopwatch.createUnstarted();

    while (durationStopwatch.elapsed(TimeUnit.MILLISECONDS) < durationMs) {
      try {
        pointStopwatch.reset();
        pointStopwatch.start();

        int numPinnedFiles = client.getPinListLength();

        pointStopwatch.stop();

        if (numPinnedFiles != mParameters.mNumFiles) {
          result.addError(String.format("Unexpected number of files: %d", numPinnedFiles));
          return result;
        }
        result.addPoint(new RpcTaskResult.Point(pointStopwatch.elapsed(TimeUnit.NANOSECONDS)));
      } catch (Exception e) {
        LOG.error("Failed when running", e);
        result.addError(e.getMessage());
      }
    }
    return result;
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new GetPinnedFileIdsBench());
  }
}
