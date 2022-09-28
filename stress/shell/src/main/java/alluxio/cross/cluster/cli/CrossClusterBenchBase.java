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

package alluxio.cross.cluster.cli;

import static alluxio.cross.cluster.cli.CrossClusterLatencyUtils.waitConsistent;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.client.metrics.MetricsMasterClient;
import alluxio.client.metrics.RetryHandlingMetricsMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.MetricValue;
import alluxio.grpc.WritePType;
import alluxio.metrics.MetricKey;
import alluxio.util.CommonUtils;
import alluxio.util.FileSystemOptions;
import alluxio.util.WaitForOptions;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

abstract class CrossClusterBenchBase {
  final List<FileSystemCrossCluster> mClients;
  final List<FileSystemCrossCluster> mRandReadClients;
  final List<MetricsMasterClient> mMetricsClients;
  final AlluxioURI mRootPath;
  final List<Long> mMountIds;
  final List<Long> mUfsOpsStartCount;
  final long mSyncLatency;
  final WritePType mWriteType;

  WaitForOptions mWaitOptions = WaitForOptions.defaults().setTimeoutMs(5000);

  final GetStatusPOptions mGetStatusOptions;
  final CreateFilePOptions mCreateFileOptions;
  final GetStatusPOptions mGetStatusOptionSync = GetStatusPOptions.newBuilder().setCommonOptions(
      FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).buildPartial()).build();
  final GetStatusPOptions mGetStatusOptionNoSync = GetStatusPOptions.newBuilder().setCommonOptions(
      FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).buildPartial()).build();

  CrossClusterBenchBase(AlluxioURI rootPath, String benchPath,
      List<List<InetSocketAddress>> clusterAddresses, long syncLatency, WritePType writeType) {
    mRootPath = rootPath.join(benchPath);
    mSyncLatency = syncLatency;
    mCreateFileOptions = CreateFilePOptions.newBuilder()
        .setWriteType(writeType).build();
    mGetStatusOptions = FileSystemOptions.getStatusDefaults(Configuration.global()).toBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setSyncIntervalMs(syncLatency).build()).build();
    mClients = clusterAddresses.stream().map(nxt -> generateClient(
        Configuration.global(), nxt)).collect(Collectors.toList());
    // use the same clients for reads
    mRandReadClients = mClients;
    mWriteType = writeType;
    mMetricsClients = clusterAddresses.stream().map(nxt -> generateMetricsClient(
        Configuration.global(), nxt)).collect(Collectors.toList());
    mUfsOpsStartCount = new ArrayList<>();
    mMountIds = new ArrayList<>();
  }

  abstract CrossClusterLatencyStatistics getClientResults(int clientID);

  abstract Long getDurationMs();

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
      mMountIds.add(cli.getStatus(mRootPath, mGetStatusOptionNoSync).getFileInfo().getMountId());
    }
    for (int i = 0; i < mMetricsClients.size(); i++) {
      mUfsOpsStartCount.add(getUfsOpsCount(mMetricsClients.get(i), mMountIds.get(i)));
    }
  }

  /**
   * Should only be used in setup and cleanup as it uses sync time 0.
   */
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

  /**
   * Should only be used in setup and cleanup as it uses sync time 0.
   */
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
    // first make sure the results are correct
    System.out.println("Waiting for clusters to be consistent");
    waitConsistent(mRootPath, mClients, mSyncLatency);

    System.out.println("Performing cleanup");
    System.out.printf("Deleting folder %s%n", mRootPath);
    mClients.get(0).delete(mRootPath, DeletePOptions.newBuilder().setAlluxioOnly(false)
        .setRecursive(true).build());

    System.out.println("Waiting for deletion to be visible on all clusters");
    for (int i = 0; i < mClients.size(); i++) {
      waitUntilDoesNotExist(mClients.get(i), mRootPath);
      mClients.get(i).close();
      mMetricsClients.get(i).close();
    }
  }

  static AlluxioURI createClusterPath(AlluxioURI rootPath, int clusterId) {
    return rootPath.join(Integer.toString(clusterId));
  }

  static AlluxioURI createFileName(AlluxioURI clusterPath, int i) {
    return clusterPath.join(Integer.toString(i));
  }

  CrossClusterLatencyStatistics[] computeResults() throws Exception {
    CrossClusterLatencyStatistics[] allResults = new CrossClusterLatencyStatistics[mClients.size()];
    for (int k = 0; k < mClients.size(); k++) {
      CrossClusterLatencyStatistics results = getClientResults(k);
      long[] ufsOpsCounter = new long[mMetricsClients.size()];
      for (int i = 0; i < mMetricsClients.size(); i++) {
        ufsOpsCounter[i] = getUfsOpsCount(mMetricsClients.get(i), mMountIds.get(i))
            - mUfsOpsStartCount.get(i);
      }
      results.setUfsOpsCountByCluster(ufsOpsCounter);
      results.recordDuration(getDurationMs());
      allResults[k] = results;
    }
    return allResults;
  }

  CrossClusterLatencyStatistics mergedResults() throws Exception {
    CrossClusterLatencyStatistics[] results = computeResults();
    CrossClusterLatencyStatistics merged = new CrossClusterLatencyStatistics();
    for (CrossClusterLatencyStatistics nxt : results) {
      merged.merge(nxt);
    }
    return merged;
  }
}
