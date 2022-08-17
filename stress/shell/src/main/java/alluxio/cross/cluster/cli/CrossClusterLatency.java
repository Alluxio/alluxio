package alluxio.cross.cluster.cli;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.client.metrics.MetricsMasterClient;
import alluxio.client.metrics.RetryHandlingMetricsMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.MetricValue;
import alluxio.grpc.WritePType;
import alluxio.master.MasterClientContext;
import alluxio.metrics.MetricKey;
import alluxio.stress.StressConstants;
import alluxio.util.FileSystemOptions;

import com.google.common.base.Stopwatch;
import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
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

  final GetStatusPOptions mGetStatusOptions;
  final CreateFilePOptions mCreateFileOptions = CreateFilePOptions.newBuilder()
      .setWriteType(WritePType.CACHE_THROUGH).build();
  final GetStatusPOptions mGetStatusOptionSync = GetStatusPOptions.newBuilder().setCommonOptions(
      FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).buildPartial()).build();

  CrossClusterLatency(AlluxioURI rootPath, List<List<InetSocketAddress>> clusterAddresses,
                      int makeFileCount, long syncLatency) {
    mMakeFileCount = makeFileCount;
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
      mClients.get(1).getStatus(mRootPath, GetStatusPOptions.newBuilder().setCommonOptions(
          FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).buildPartial()).build());
    } catch (FileDoesNotExistException e) {
      // OK because should be deleted
    }
    mClients.get(0).createDirectory(mRootPath, CreateDirectoryPOptions.newBuilder()
        .setWriteType(WritePType.CACHE_THROUGH).build());

    mClients.get(1).getStatus(mRootPath, mGetStatusOptionSync);

    for (FileSystemCrossCluster cli : mClients) {
      mMountIds.add(cli.getStatus(mRootPath, mGetStatusOptionSync).getFileInfo().getMountId());
    }
    for (int i = 0; i < mMetricsClients.size(); i++) {
      mUfsOpsStartCount.add(getUfsOpsCount(mMetricsClients.get(i), mMountIds.get(i)));
    }
  }

  long getUfsOpsCount(MetricsMasterClient client, long mountId) throws Exception {
    MetricValue metric = client.getMetrics().get(MetricKey.getSyncMetricName(mountId));
    if (metric != null) {
      return (long) metric.getDoubleValue();
    }
    return 0;
  }

  void doCleanup() throws IOException, AlluxioException {
    mClients.get(0).delete(mRootPath, DeletePOptions.newBuilder().setAlluxioOnly(false)
        .setRecursive(true).build());
    try {
      mClients.get(1).getStatus(mRootPath, GetStatusPOptions.newBuilder().setCommonOptions(
          FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).buildPartial()).build());
    } catch (FileDoesNotExistException e) {
      // OK because should be deleted
    }
    for (int i = 0; i < mClients.size(); i++) {
      mClients.get(i).close();
      mMetricsClients.get(i).close();
    }
  }

  void run() {
    Thread writer = new Thread(() -> {
      try {
        runWriter(mClients.get(0));
      } catch (IOException e) {
        e.printStackTrace();
      } catch (AlluxioException e) {
        throw new RuntimeException(e);
      }
    });
    Thread reader = new Thread(() -> {
      try {
        runReader(mClients.get(1));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    writer.start();
    reader.start();
    try {
      writer.join();
      reader.join();
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

  void runWriter(FileSystemCrossCluster client) throws IOException, AlluxioException {
    for (int i = 0; i < mMakeFileCount; i++) {
      client.createFile(createFileName(mRootPath, i), mCreateFileOptions).close();
      mTimers.set(i, Stopwatch.createStarted());
    }
  }

  void runReader(FileSystemCrossCluster client) throws IOException, AlluxioException {
    for (int i = 0; i < mMakeFileCount; i++) {
      while (mTimers.get(i) == null) {}
      while (true) {
        try {
          client.getStatus(createFileName(mRootPath, i));
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
