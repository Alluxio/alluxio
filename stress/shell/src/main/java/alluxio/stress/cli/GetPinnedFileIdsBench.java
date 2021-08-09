package alluxio.stress.cli;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterWorkerServiceGrpc;
import alluxio.grpc.GetPinnedFileIdsPRequest;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.MasterClientContext;
import alluxio.resource.CloseableResource;
import alluxio.stress.rpc.GetPinnedFileIdsParameters;
import alluxio.stress.rpc.RpcTaskResult;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A benchmarking tool for the GetPinnedFileIds RPC.
 * The test will generate a specified number of test files and pin them.
 * Then it will keep calling the GetPinnedFileIds RPC by the specified load until the specified
 * duration has elapsed.
 *
 * Example:
 * 4 simulated workers running on 2 job workers, requesting a total number of 100k pinned files
 * for a total of 5 seconds:
 * $ bin/alluxio runClass alluxio.stress.cli.GetPinnedFileIdsBench --concurrency 2
 *    --cluster-limit 2 --num-files 100000 --duration 5s
 */
public class GetPinnedFileIdsBench extends RpcBench<GetPinnedFileIdsParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(GetPinnedFileIdsBench.class);

  @ParametersDelegate
  private GetPinnedFileIdsParameters mParameters = new GetPinnedFileIdsParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();
  private final FileSystemContext mFileSystemContext = FileSystemContext.create(mConf);
  private final AlluxioURI mBaseUri = new AlluxioURI(mParameters.mBasePath);
  private final Stopwatch mDurationStopwatch = Stopwatch.createUnstarted();
  private final Stopwatch mPointStopwatch = Stopwatch.createUnstarted();
  private final long mDurationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
  private final PinListFileSystemMasterClient mWorkerClient =
      new PinListFileSystemMasterClient(
          MasterClientContext.newBuilder(ClientContext.create(mConf)).build());

  @Override
  public void prepare() throws Exception {
    // The task ID is different for local and cluster executions
    // So including that in the log can help associate the log to the run
    LOG.info("Task ID is {}", mBaseParameters.mId);

    try (CloseableResource<alluxio.client.file.FileSystemMasterClient> client =
             mFileSystemContext.acquireMasterClientResource()) {
      LOG.info("Creating temporary directory {} for benchmark", mBaseUri);
      client.get().createDirectory(mBaseUri,
              CreateDirectoryPOptions.newBuilder().setAllowExists(true).build());
    }
    int numFiles = mParameters.mNumFiles;
    int fileNameLength = (int) Math.max(8, Math.log10(numFiles));

    CompletableFuture<Void>[] futures = new CompletableFuture[numFiles];
    LOG.info("Generating {} pinned test files at the master", numFiles);
    for (int i = 0; i < numFiles; i++) {
      AlluxioURI fileUri = mBaseUri.join(CommonUtils.randomAlphaNumString(fileNameLength));
      CompletableFuture<Void> future = CompletableFuture.supplyAsync((Supplier<Void>) () -> {
        try (CloseableResource<FileSystemMasterClient> client =
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
        }
        return null;
      }, getPool());
      futures[i] = (future);
    }
    CompletableFuture.allOf(futures).join();
    LOG.info("Test files generated");
  }

  @Override
  public void cleanup() throws Exception {
    try (CloseableResource<alluxio.client.file.FileSystemMasterClient> client =
             mFileSystemContext.acquireMasterClientResource()) {
      LOG.info("Deleting test directory {}", mBaseUri);
      client.get().delete(mBaseUri, DeletePOptions.newBuilder().setRecursive(true).build());
    }
    super.cleanup();
  }

  @Override
  public RpcTaskResult runRPC() throws Exception {
    RpcTaskResult result = new RpcTaskResult();
    result.setBaseParameters(mBaseParameters);
    result.setParameters(mParameters);

    mDurationStopwatch.reset().start();
    while (mDurationStopwatch.elapsed(TimeUnit.MILLISECONDS) < mDurationMs) {
      try {
        mPointStopwatch.reset().start();

        int numPinnedFiles = mWorkerClient.getPinListLength();

        mPointStopwatch.stop();

        if (numPinnedFiles != mParameters.mNumFiles) {
          result.addError(String.format("Unexpected number of files: %d, expected %d",
              numPinnedFiles, mParameters.mNumFiles));
          return result;
        }
        result.addPoint(new RpcTaskResult.Point(mPointStopwatch.elapsed(TimeUnit.NANOSECONDS)));
      } catch (Exception e) {
        LOG.error("Failed when running", e);
        result.addError(e.getMessage());
      }
    }
    return result;
  }

  @Override
  public GetPinnedFileIdsParameters getParameters() {
    return mParameters;
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new GetPinnedFileIdsBench());
  }

  /**
   * A helper client that is used to minimize the deserialization overhead by calling
   * getPinnedFileIdsCount instead of getPinnedFileIdsList.
   *
   * By calling getPinnedFileIdsList, GRPC deserializes the IDs from the response, which in this
   * benchmark are irrelevant, since we only care about the length of the list. By getting only
   * the count of the IDs, we avoid the deserialization of the IDs.
   * We want to minimize the client side cost while measuring the RPC performance, so that the
   * collected results can get as close as possible to what the server side
   * (in this benchmark's case the master) can achieve.
   */
  private static class PinListFileSystemMasterClient
      extends alluxio.worker.file.FileSystemMasterClient {
    private static final Logger LOG =
        LoggerFactory.getLogger(PinListFileSystemMasterClient.class);
    private FileSystemMasterWorkerServiceGrpc.FileSystemMasterWorkerServiceBlockingStub mClient =
        null;

    public PinListFileSystemMasterClient(MasterClientContext conf) {
      super(conf);
    }

    @Override
    protected void afterConnect() throws IOException {
      mClient = FileSystemMasterWorkerServiceGrpc.newBlockingStub(mChannel);
    }

    public int getPinListLength() throws IOException {
      return retryRPC(() -> mClient
          .withDeadlineAfter(
              mContext.getClusterConf().getMs(PropertyKey.WORKER_MASTER_PERIODICAL_RPC_TIMEOUT),
              TimeUnit.MILLISECONDS)
          .getPinnedFileIds(GetPinnedFileIdsPRequest.newBuilder().build()).getPinnedFileIdsCount(),
          LOG, "GetPinList", "");
    }
  }
}
