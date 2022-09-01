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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

class CrossClusterLatency {

  final List<FileSystemCrossCluster> mClients;
  final List<MetricsMasterClient> mMetricsClients;
  final int mMakeFileCount;
  final AlluxioURI mRootPath;
  final List<List<Long>> mTimers;
  final List<Long> mMountIds;
  final List<Long> mUfsOpsStartCount;
  final boolean mRandReader;
  List<List<List<Long>>> mCheckResults;
  final List<RandResult> mRandResult = new ArrayList<>();
  WaitForOptions mWaitOptions = WaitForOptions.defaults().setTimeoutMs(5000);
  long mDuration;

  final GetStatusPOptions mGetStatusOptions;
  final CreateFilePOptions mCreateFileOptions = CreateFilePOptions.newBuilder()
      .setWriteType(WritePType.CACHE_THROUGH).build();
  final GetStatusPOptions mGetStatusOptionSync = GetStatusPOptions.newBuilder().setCommonOptions(
      FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).buildPartial()).build();

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
    mTimers = new ArrayList<>(mClients.size());
    for (int i = 0; i < mClients.size(); i++) {
      mTimers.add(new ArrayList<>(makeFileCount));
    }
    mUfsOpsStartCount = new ArrayList<>();
    mMountIds = new ArrayList<>();
  }

  void doSetup() throws Exception {

    for (int i = 0; i < mClients.size(); i++) {
      // delete the path on the owner cluster
      try {
        mClients.get(i).delete(createClusterPath(mRootPath, i),
            DeletePOptions.newBuilder().setAlluxioOnly(false)
            .setRecursive(true).build());
      } catch (FileDoesNotExistException e) {
        // OK because should be deleted
      }
      // wait for the path not to exist on all clusters
      for (FileSystemCrossCluster client : mClients) {
        waitUntilDoesNotExist(client, createClusterPath(mRootPath, i));
      }
      // create the path on the owner cluster
      mClients.get(i).createDirectory(createClusterPath(mRootPath, i),
          CreateDirectoryPOptions.newBuilder().setRecursive(true)
              .setWriteType(WritePType.CACHE_THROUGH).build());
      // wait for the path to exist on all clusters
      for (FileSystemCrossCluster client : mClients) {
        waitUntilExists(client, createClusterPath(mRootPath, i));
      }
    }

    // track the UFS ops count for each cluster by the mount id
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

    for (int i = 0; i < mClients.size(); i++) {
      waitUntilDoesNotExist(mClients.get(i), mRootPath);
      mClients.get(i).close();
      mMetricsClients.get(i).close();
    }
  }

  void run() {
    List<Thread> writerThreads = new ArrayList<>();
    List<Thread> randReaderThreads = new ArrayList<>();
    List<ExecutorService> executors = new ArrayList<>();
    mCheckResults = new ArrayList<>();
    for (int i = 0; i < mClients.size(); i++) {
      FileSystemCrossCluster mainClient = mClients.get(i);
      List<FileSystemCrossCluster> otherClients = new ArrayList<>();
      for (FileSystemCrossCluster client : mClients) {
        if (client != mainClient) {
          otherClients.add(client);
        }
      }
      AlluxioURI path = createClusterPath(mRootPath, i);
      AtomicBoolean running = new AtomicBoolean(true);
      ExecutorService executor = Executors.newFixedThreadPool(mClients.size() - 1);
      executors.add(executor);
      List<Long> results = mTimers.get(i);
      mCheckResults.add(new ArrayList<>());
      for (int j = 0; j < otherClients.size(); j++) {
        mCheckResults.get(i).add(new ArrayList<>());
      }
      int finalI = i;
      writerThreads.add(new Thread(() -> {
        try {
          runWriter(path, results, mCheckResults.get(finalI), mainClient, otherClients, executor);
        } catch (IOException e) {
          e.printStackTrace();
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          running.set(false);
        }
      }));
      if (mRandReader) {
        for (FileSystemCrossCluster client : otherClients) {
          RandResult randResult = new RandResult();
          mRandResult.add(randResult);
          randReaderThreads.add(new Thread(() -> {
            try {
              runRandAccess(path, client, randResult, running);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }));
        }
      }
    }
    Stopwatch duration = Stopwatch.createStarted();
    for (Thread writer : writerThreads) {
      writer.start();
    }
    for (Thread randReader : randReaderThreads) {
      randReader.start();
    }
    try {
      for (Thread writer : writerThreads) {
        writer.join();
      }
      for (Thread randReader : randReaderThreads) {
        randReader.join();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      mDuration = duration.elapsed(TimeUnit.MILLISECONDS);
      for (ExecutorService executor : executors) {
        executor.shutdown();
      }
    }
  }

  LatencyResultsStatistics computeAllReadResults() {
    Histogram normalReads = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
        StressConstants.TIME_HISTOGRAM_PRECISION);
    LatencyResultsStatistics results = new LatencyResultsStatistics();
    mCheckResults.stream().flatMap(Collection::stream).flatMap(Collection::stream)
        .forEach(nxt -> {
          normalReads.recordValue(nxt);
          results.recordResult(nxt);
        });
    results.recordDuration(mDuration);
    results.encodeResponseTimeNsRaw(normalReads);
    return results;
  }

  CrossClusterLatencyStatistics[] computeResults() throws Exception {
    CrossClusterLatencyStatistics[] allResults = new CrossClusterLatencyStatistics[mClients.size()];
    for (int k = 0; k < mClients.size(); k++) {
      CrossClusterLatencyStatistics results = new CrossClusterLatencyStatistics();
      allResults[k] = results;
      long[] ufsOpsCounter = new long[mMetricsClients.size()];
      for (int i = 0; i < mMetricsClients.size(); i++) {
        ufsOpsCounter[i] = getUfsOpsCount(mMetricsClients.get(i), mMountIds.get(i))
            - mUfsOpsStartCount.get(i);
      }
      results.setUfsOpsCountByCluster(ufsOpsCounter);
      results.recordDuration(mDuration);
      if (mRandReader) {
        mRandResult.get(k).setDuration(mDuration);
        results.setRandResult(mRandResult.get(k));
      }
      Histogram latencies = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
          StressConstants.TIME_HISTOGRAM_PRECISION);
      for (Long nxtValue : mTimers.get(k)) {
        latencies.recordValue(nxtValue);
        results.recordResult(nxtValue);
      }
      results.encodeResponseTimeNsRaw(latencies);
    }
    return allResults;
  }

  static AlluxioURI createClusterPath(AlluxioURI rootPath, int clusterId) {
    return rootPath.join(Integer.toString(clusterId));
  }

  static AlluxioURI createFileName(AlluxioURI clusterPath, int i) {
    return clusterPath.join(Integer.toString(i));
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

  void runRandAccess(AlluxioURI rootPath, FileSystemCrossCluster client,
                     RandResult result, AtomicBoolean running)
      throws Exception {
    Random rand = new Random();
    while (running.get()) {
      try {
        client.getStatus(createFileName(rootPath, rand.nextInt(mMakeFileCount)),
            mGetStatusOptions);
      } catch (FileDoesNotExistException e) {
        result.mFailures++;
        continue;
      }
      result.mSuccess++;
    }
  }

  void runWriter(
      AlluxioURI rootPath, List<Long> latencyResults, List<List<Long>> checkResults,
      FileSystemCrossCluster writeClient, List<FileSystemCrossCluster> readClients,
      ExecutorService executor) throws IOException, AlluxioException,
      ExecutionException, InterruptedException {
    Stopwatch timer = Stopwatch.createUnstarted();
    List<Future<Void>> readResults = new ArrayList(Arrays.asList(new Object[readClients.size()]));
    List<Stopwatch> clocks = new ArrayList<>();
    for (int i = 0; i < readClients.size(); i++) {
      clocks.add(Stopwatch.createUnstarted());
    }
    for (int i = 0; i < mMakeFileCount; i++) {
      int finalI = i;
      // read the file on each cluster to check that it does not exist
      for (int j = 0; j < readClients.size(); j++) {
        int finalJ = j;
        readResults.set(j, executor.submit(() -> {
          clocks.get(finalJ).reset().start();
          try {
            readClients.get(finalJ).getStatus(createFileName(rootPath, finalI), mGetStatusOptions);
          } catch (FileDoesNotExistException e) {
            // OK
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          checkResults.get(finalJ).add(clocks.get(finalJ).elapsed(TimeUnit.NANOSECONDS));
          return null;
        }));
      }
      // wait for the reads to complete
      for (Future<Void> readResult : readResults) {
        readResult.get();
      }
      // write the file on the home cluster
      writeClient.createFile(createFileName(rootPath, i), mCreateFileOptions).close();
      // repeatably read the file on each cluster until it exists
      timer.reset().start();
      for (int j = 0; j < readClients.size(); j++) {
        int finalJ = j;
        readResults.set(j, executor.submit(() -> {
          while (true) {
            clocks.get(finalJ).reset().start();
            try {
              readClients.get(finalJ).getStatus(createFileName(rootPath, finalI),
                  mGetStatusOptions);
            } catch (FileDoesNotExistException e) {
              clocks.get(finalJ).elapsed(TimeUnit.NANOSECONDS);
              continue;
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            clocks.get(finalJ).elapsed(TimeUnit.NANOSECONDS);
            break;
          }
          return null;
        }));
      }
      // wait until all reads complete
      for (Future<Void> readResult : readResults) {
        readResult.get();
      }
      timer.stop();
      latencyResults.add(timer.elapsed(TimeUnit.NANOSECONDS));
      System.out.printf("Latency (ms): %d%n", timer.elapsed(TimeUnit.MILLISECONDS));
    }
  }
}
