package alluxio.cross.cluster.cli;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.client.metrics.MetricsMasterClient;
import alluxio.client.metrics.RetryHandlingMetricsMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.MetricValue;
import alluxio.grpc.WritePType;
import alluxio.metrics.MetricKey;
import alluxio.stress.StressConstants;
import alluxio.util.CommonUtils;
import alluxio.util.FileSystemOptions;
import alluxio.util.WaitForOptions;

import com.google.common.base.Stopwatch;
import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

class CrossClusterLatency {

  final List<FileSystemCrossCluster> mClients;
  final List<MetricsMasterClient> mMetricsClients;
  final int mMakeFileCount;
  final AlluxioURI mRootPath;
  final AtomicReferenceArray<Stopwatch> mTimers;
  final List<Long> mMountIds;
  final List<Long> mUfsOpsStartCount;
  final boolean mRandReader;
  final RandResult mRandResult = new RandResult();
  WaitForOptions mWaitOptions = WaitForOptions.defaults().setTimeoutMs(5000);

  final GetStatusPOptions mGetStatusOptions;
  final CreateFilePOptions mCreateFileOptions = CreateFilePOptions.newBuilder()
      .setWriteType(WritePType.CACHE_THROUGH).build();
  final GetStatusPOptions mGetStatusOptionSync = GetStatusPOptions.newBuilder().setCommonOptions(
      FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).buildPartial()).build();

  private volatile boolean mRunning = true;

  CrossClusterLatency(AlluxioURI rootPath, List<List<InetSocketAddress>> clusterAddresses,
                      int makeFileCount, long syncLatency, boolean runRandReader) {
    mMakeFileCount = makeFileCount;
    mRandReader = runRandReader;
    mRootPath = rootPath.join("latencyFiles");
    mGetStatusOptions = FileSystemOptions.getStatusDefaults(Configuration.global()).toBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setSyncIntervalMs(syncLatency).build()).build();
    mClients = clusterAddresses.stream().map(nxt -> generateClient(
        Configuration.global(), nxt)).collect(Collectors.toList());
    mMetricsClients = clusterAddresses.stream().map(nxt -> generateMetricsClient(
        Configuration.global(), nxt)).collect(Collectors.toList());
    mTimers = new AtomicReferenceArray<>(mMakeFileCount);
    mUfsOpsStartCount = new ArrayList<>();
    mMountIds = new ArrayList<>();
  }

  void doSetup() throws Exception {
    try {
      mClients.get(0).delete(mRootPath, DeletePOptions.newBuilder().setAlluxioOnly(false)
          .setRecursive(true).build());
    } catch (FileDoesNotExistException e) {
      // OK because should be deleted
    }
    waitUntilDoesNotExist(mClients.get(1), mRootPath);

    mClients.get(0).createDirectory(mRootPath, CreateDirectoryPOptions.newBuilder()
        .setWriteType(WritePType.CACHE_THROUGH).build());
    waitUntilExists(mClients.get(1), mRootPath);

    for (FileSystemCrossCluster cli : mClients) {
      mMountIds.add(cli.getStatus(mRootPath, mGetStatusOptionSync).getFileInfo().getMountId());
    }
    for (int i = 0; i < mMetricsClients.size(); i++) {
      mUfsOpsStartCount.add(getUfsOpsCount(mMetricsClients.get(i), mMountIds.get(i)));
    }
  }

  void waitUntilExists(FileSystemCrossCluster client, AlluxioURI path) throws Exception {
    CommonUtils.waitFor(String.format("Path %s created", path), () -> {
      try {
        client.getStatus(path, mGetStatusOptionSync);
      } catch (FileDoesNotExistException e) {
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return true;
    }, mWaitOptions);
  }

  void waitUntilDoesNotExist(FileSystemCrossCluster client, AlluxioURI path) throws Exception {
    CommonUtils.waitFor(String.format("Path %s removed", path), () -> {
      try {
        client.getStatus(path, mGetStatusOptionSync);
      } catch (FileDoesNotExistException e) {
        return true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return false;
    }, mWaitOptions);
  }

  long getUfsOpsCount(MetricsMasterClient client, long mountId) throws Exception {
    MetricValue metric = client.getMetrics().get(MetricKey.getSyncMetricName(mountId));
    if (metric != null) {
      return (long) metric.getDoubleValue();
    }
    return 0;
  }

  void doCleanup() throws Exception {
    mClients.get(0).delete(mRootPath, DeletePOptions.newBuilder().setAlluxioOnly(false)
        .setRecursive(true).build());
    waitUntilDoesNotExist(mClients.get(1), mRootPath);

    for (int i = 0; i < mClients.size(); i++) {
      mClients.get(i).close();
      mMetricsClients.get(i).close();
    }
  }

  void run() {
    Thread writer = new Thread(() -> {
      try {
        runWriter(mClients.get(0), mClients.get(1));
      } catch (IOException e) {
        e.printStackTrace();
      } catch (AlluxioException e) {
        throw new RuntimeException(e);
      }
    });
    Thread randReader = new Thread(() -> {
      try {
        runRandAccess(mClients.get(1), mRandResult);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    writer.start();
    if (mRandReader) {
      randReader.start();
    }
    try {
      writer.join();
      mRunning = false;
      randReader.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  CrossClusterLatencyStatistics computeResults() throws Exception {
    CrossClusterLatencyStatistics results = new CrossClusterLatencyStatistics();
    long[] ufsOpsCounter = new long[mMetricsClients.size()];
    for (int i = 0; i < mMetricsClients.size(); i++) {
      ufsOpsCounter[i] = getUfsOpsCount(mMetricsClients.get(i), mMountIds.get(i))
          - mUfsOpsStartCount.get(i);
    }
    results.setUfsOpsCountByCluster(ufsOpsCounter);
    results.setRandResult(mRandResult);
    Histogram latencies = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
        StressConstants.TIME_HISTOGRAM_PRECISION);
    for (int i = 0; i < mMakeFileCount; i++) {
      Stopwatch next = mTimers.get(i);
      if (next == null || next.isRunning()) {
        break;
      }
      long nxtValue = next.elapsed(TimeUnit.NANOSECONDS);
      latencies.recordValue(nxtValue);
      results.recordResult(nxtValue);
    }
    results.encodeResponseTimeNsRaw(latencies);
    return results;
  }

  static AlluxioURI createFileName(AlluxioURI rootPath, int i) {
    return rootPath.join(Integer.toString(i));
  }

  static FileSystemCrossCluster generateClient(
      AlluxioConfiguration conf, List<InetSocketAddress> masterAddresses) {
    return FileSystemCrossCluster.Factory.create(FileSystemContext.create(conf,
        masterAddresses));
  }

  static MetricsMasterClient generateMetricsClient(
      AlluxioConfiguration conf, List<InetSocketAddress> masterAddresses) {
    return new RetryHandlingMetricsMasterClient(FileSystemContext.create(conf,
        masterAddresses).getMasterClientContext());
  }

  void runRandAccess(FileSystemCrossCluster client, RandResult result) throws Exception {
    Random rand = new Random();
    while (mRunning) {
      try {
        client.getStatus(createFileName(mRootPath, rand.nextInt(mMakeFileCount)),
            mGetStatusOptions);
      } catch (FileDoesNotExistException e) {
        result.mFailures++;
        continue;
      }
      result.mSuccess++;
    }
  }

  void runWriter(FileSystemCrossCluster writeClient, FileSystemCrossCluster readClient)
      throws IOException, AlluxioException {
    for (int i = 0; i < mMakeFileCount; i++) {
      try {
        readClient.getStatus(createFileName(mRootPath, i), mGetStatusOptions);
      } catch (FileDoesNotExistException e) {
        // OK
      }
      writeClient.createFile(createFileName(mRootPath, i), mCreateFileOptions).close();
      mTimers.set(i, Stopwatch.createStarted());
      while (true) {
        try {
          readClient.getStatus(createFileName(mRootPath, i), mGetStatusOptions);
        } catch (FileDoesNotExistException e) {
          continue;
        }
        mTimers.get(i).stop();
        System.out.printf("Latency (ms): %d\n", mTimers.get(i).elapsed(TimeUnit.MILLISECONDS));
        break;
      }
    }
  }
}
