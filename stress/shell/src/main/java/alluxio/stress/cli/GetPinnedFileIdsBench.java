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
    try (CloseableResource<alluxio.client.file.FileSystemMasterClient> client =
             mFileSystemContext.acquireMasterClientResource()) {
      client.get().createDirectory(mBaseUri,
              CreateDirectoryPOptions.newBuilder().setAllowExists(true).build());
    }
    int numFiles = mParameters.mNumFiles;
    int fileNameLength = (int) Math.max(8, Math.log10(numFiles));

    CompletableFuture<Void>[] futures = new CompletableFuture[numFiles];
    LOG.info("Generating {} test files", numFiles);
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
          result.addError(String.format("Unexpected number of files: %d", numPinnedFiles));
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
   * Helper client that is used to minimizes the deserialization overhead by calling
   * getPinnedFileIdsCount instead of getPinnedFileIdsList
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
