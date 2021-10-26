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
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A benchmarking tool for the GetPinnedFileIds RPC.
 */
public class GetPinnedFileIdsBench extends RpcBench<GetPinnedFileIdsParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(GetPinnedFileIdsBench.class);

  @ParametersDelegate
  private final GetPinnedFileIdsParameters mParameters = new GetPinnedFileIdsParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();
  private final FileSystemContext mFileSystemContext = FileSystemContext.create(mConf);
  private final ThreadLocal<Stopwatch> mDurationStopwatch =
      ThreadLocal.withInitial(Stopwatch::createUnstarted);
  private final ThreadLocal<Stopwatch> mPointStopwatch =
      ThreadLocal.withInitial(Stopwatch::createUnstarted);
  private final PinListFileSystemMasterClient mWorkerClient =
      new PinListFileSystemMasterClient(
          MasterClientContext.newBuilder(ClientContext.create(mConf)).build());

  @Override
  public String getBenchDescription() {
    return String.join("\n", ImmutableList.of(
        "A benchmarking tool for the GetPinnedFileIds RPC.",
        "The test will generate a specified number of test files and pin them. "
            + "Then it will keep calling the GetPinnedFileIds RPC by the specified load until the "
            + "specified duration has elapsed. The test files will be cleaned up in the end.",
        "",
        "Example:",
        "# 2 job workers will be chosen to run the benchmark",
        "# Each job worker runs 3 simulated clients",
        "# Each client keeps requesting a total number of 10k pinned files for "
            + "a total of 100 milliseconds",
        "$ bin/alluxio runClass alluxio.stress.cli.GetPinnedFileIdsBench --concurrency 3 \\",
        "--cluster --cluster-limit 2 --num-files 10000 --duration 100ms",
        ""
    ));
  }

  @Override
  public void prepare() throws Exception {
    // The task ID is different for local and cluster executions
    // So including that in the log can help associate the log to the run
    LOG.info("Task ID is {}", mBaseParameters.mId);

    // the preparation is done by the invoking client
    // so skip preparation when running in job worker
    if (mBaseParameters.mDistributed) {
      LOG.info("Skipping preparation in distributed execution");
      return;
    }

    AlluxioURI baseUri = new AlluxioURI(mParameters.mBasePath);
    try (CloseableResource<alluxio.client.file.FileSystemMasterClient> client =
             mFileSystemContext.acquireMasterClientResource()) {
      LOG.info("Creating temporary directory {} for benchmark", baseUri);
      client.get().createDirectory(baseUri,
              CreateDirectoryPOptions.newBuilder().setAllowExists(true).build());
    }
    int numFiles = mParameters.mNumFiles;
    int fileNameLength = (int) Math.ceil(Math.max(8, Math.log10(numFiles)));

    CompletableFuture<Void>[] futures = new CompletableFuture[numFiles];
    LOG.info("Generating {} pinned test files at the master", numFiles);
    for (int i = 0; i < numFiles; i++) {
      AlluxioURI fileUri = baseUri.join(CommonUtils.randomAlphaNumString(fileNameLength));
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
    // skip cleanup in job worker as the test files are to be cleaned up by the client
    if (mBaseParameters.mDistributed) {
      LOG.info("Skipping cleanup in distributed execution");
    } else {
      AlluxioURI baseUri = new AlluxioURI(mParameters.mBasePath);
      try (CloseableResource<FileSystemMasterClient> client =
               mFileSystemContext.acquireMasterClientResource()) {
        LOG.info("Deleting test directory {}", baseUri);
        client.get().delete(baseUri, DeletePOptions.newBuilder().setRecursive(true).build());
      } catch (AlluxioStatusException e) {
        LOG.warn("Failed to delete test directory {}, manual cleanup needed", baseUri, e);
      }
    }
    super.cleanup();
  }

  @Override
  public RpcTaskResult runRPC() throws Exception {
    RpcTaskResult result = new RpcTaskResult();

    mDurationStopwatch.get().reset().start();
    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    LOG.info("Beginning benchmark, running for {} ms", durationMs);
    while (mDurationStopwatch.get().elapsed(TimeUnit.MILLISECONDS) < durationMs) {
      try {
        mPointStopwatch.get().reset().start();

        int numPinnedFiles = mWorkerClient.getPinListLength();

        mPointStopwatch.get().stop();

        if (numPinnedFiles != mParameters.mNumFiles) {
          result.addError(String.format("Unexpected number of files: %d, expected %d",
              numPinnedFiles, mParameters.mNumFiles));
          continue;
        }
        result.addPoint(
            new RpcTaskResult.Point(mPointStopwatch.get().elapsed(TimeUnit.MILLISECONDS)));
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
