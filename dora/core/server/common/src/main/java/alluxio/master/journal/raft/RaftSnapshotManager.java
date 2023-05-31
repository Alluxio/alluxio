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

package alluxio.master.journal.raft;

import alluxio.AbstractClient;
import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.SnapshotData;
import alluxio.grpc.SnapshotMetadata;
import alluxio.master.selectionpolicy.MasterSelectionPolicy;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.util.ConfigurationUtils;
import alluxio.util.compression.DirectoryMarshaller;
import alluxio.util.logging.SamplingLogger;
import alluxio.util.network.NetworkAddressUtils;

import com.codahale.metrics.Timer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Manages a snapshot download.
 */
public class RaftSnapshotManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RaftSnapshotManager.class);
  private static final Logger SAMPLING_LOG = new SamplingLogger(LOG, 10L * Constants.SECOND_MS);

  private final int mRequestInfoTimeout = (int)
      Configuration.getMs(PropertyKey.MASTER_JOURNAL_REQUEST_INFO_TIMEOUT);

  private final SnapshotDirStateMachineStorage mStorage;
  private final ExecutorService mExecutor;
  private final Map<InetSocketAddress, RaftJournalServiceClient> mClients;

  private volatile long mLastSnapshotDownloadDurationMs = -1;
  private volatile long mLastSnapshotDownloadSize = -1;
  private volatile long mLastSnapshotDownloadDiskSize = -1;

  @Nullable
  private CompletableFuture<Long> mDownloadFuture = null;

  RaftSnapshotManager(SnapshotDirStateMachineStorage storage, ExecutorService executor) {
    mStorage = storage;
    mExecutor = executor;

    InetSocketAddress localAddress = NetworkAddressUtils.getConnectAddress(
        NetworkAddressUtils.ServiceType.MASTER_RPC, Configuration.global());
    mClients = ConfigurationUtils.getMasterRpcAddresses(Configuration.global()).stream()
        .filter(address -> !address.equals(localAddress))
        .collect(Collectors.toMap(Function.identity(), address -> {
          MasterSelectionPolicy selection = MasterSelectionPolicy.Factory.specifiedMaster(address);
          int numTries = 10;
          int sleep = Math.max(1, mRequestInfoTimeout / numTries);
          // try to connect to other master once per second for until request info timeout
          Supplier<RetryPolicy> retry = () -> new ExponentialBackoffRetry(sleep, sleep, numTries);
          return new RaftJournalServiceClient(selection, retry);
        }));

    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EMBEDDED_JOURNAL_LAST_SNAPSHOT_DOWNLOAD_DURATION_MS.getName(),
        () -> mLastSnapshotDownloadDurationMs);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EMBEDDED_JOURNAL_LAST_SNAPSHOT_DOWNLOAD_SIZE.getName(),
        () -> mLastSnapshotDownloadSize);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EMBEDDED_JOURNAL_LAST_SNAPSHOT_DOWNLOAD_DISK_SIZE.getName(),
        () -> mLastSnapshotDownloadDiskSize);
  }

  /**
   * Waits synchronously for the download attempt to be complete.
   * @return the result of the download attempt, or {@link RaftLog#INVALID_LOG_INDEX} if no
   * attempt is underway
   */
  public long waitForAttemptToComplete() {
    if (mDownloadFuture == null) {
      return RaftLog.INVALID_LOG_INDEX;
    }
    mDownloadFuture.join();
    // this is to make sure that mDownloadFuture gets reset to null
    return downloadSnapshotFromOtherMasters();
  }

  /**
   * Launches an asynchronous download of the most updated snapshot found on other masters in the
   * cluster. If the asynchronous download is already in flight, it polls for the results.
   * @return the log index of the last successful snapshot installation, or -1 if the download is in
   * flight or has failed.
   */
  public long downloadSnapshotFromOtherMasters() {
    if (mClients.isEmpty()) {
      SAMPLING_LOG.warn("No followers are present to download a snapshot from");
      return RaftLog.INVALID_LOG_INDEX;
    }
    if (mDownloadFuture == null) {
      mDownloadFuture = CompletableFuture.supplyAsync(this::core, mExecutor).exceptionally(err -> {
        LOG.debug("Failed to download snapshot", err);
        return RaftLog.INVALID_LOG_INDEX;
      });
    } else if (mDownloadFuture.isDone()) {
      LOG.debug("Download operation is done");
      Long snapshotIndex = mDownloadFuture.join();
      LOG.debug("Retrieved downloaded snapshot at index {}", snapshotIndex);
      mDownloadFuture = null;
      return snapshotIndex;
    }
    return RaftLog.INVALID_LOG_INDEX;
  }

  private long core() {
    SnapshotInfo localSnapshotInfo = mStorage.getLatestSnapshot();
    if (localSnapshotInfo == null) {
      LOG.info("No local snapshot found");
    } else {
      LOG.info("Local snapshot is {}", TermIndex.valueOf(localSnapshotInfo.getTerm(),
          localSnapshotInfo.getIndex()));
    }
    // max heap based on TermIndex extracted from the SnapshotMetadata of each pair
    PriorityQueue<ImmutablePair<SnapshotMetadata, InetSocketAddress>> otherInfos =
        new PriorityQueue<>(Math.max(1, mClients.size()),
            Collections.reverseOrder(Comparator.comparing(pair -> toTermIndex(pair.getLeft()))));
    // wait mRequestInfoTimeout between each attempt to contact the masters
    RetryPolicy retryPolicy =
        new ExponentialBackoffRetry(mRequestInfoTimeout, mRequestInfoTimeout, 10);
    while (otherInfos.isEmpty() && retryPolicy.attempt()) {
      LOG.debug("Attempt to retrieve info");
      otherInfos.addAll(retrieveFollowerInfos(localSnapshotInfo));
      LOG.debug("Attempt to retrieve info over");
    }

    while (!otherInfos.isEmpty()) {
      ImmutablePair<SnapshotMetadata, InetSocketAddress> info = otherInfos.poll();
      InetSocketAddress address = info.getRight();
      SnapshotMetadata snapshotMetadata = info.getLeft();
      long index;
      if ((index = downloadSnapshotFromAddress(snapshotMetadata, address))
          != RaftLog.INVALID_LOG_INDEX) {
        return index;
      }
    }
    return RaftLog.INVALID_LOG_INDEX;
  }

  /**
   * @param localSnapshotInfo contains information about the most up-to-date snapshot on this master
   * @return a sorted list of pairs containing a follower's address and its most up-to-date snapshot
   */
  private List<ImmutablePair<SnapshotMetadata, InetSocketAddress>> retrieveFollowerInfos(
      SnapshotInfo localSnapshotInfo) {
    return mClients.keySet().parallelStream()
        // map to a pair of (address, SnapshotMetadata) by requesting all followers in parallel
        .map(address -> {
          RaftJournalServiceClient client = mClients.get(address);
          try {
            client.connect();
            LOG.info("Receiving snapshot info from {}", address);
            SnapshotMetadata metadata = client.requestLatestSnapshotInfo();
            if (!metadata.getExists()) {
              LOG.info("No snapshot is present on {}", address);
            } else {
              LOG.info("Received snapshot info {} from {}", toTermIndex(metadata), address);
            }
            return ImmutablePair.of(metadata, address);
          } catch (Exception e) {
            client.disconnect();
            LOG.debug("Failed to retrieve snapshot info from {}", address, e);
            return ImmutablePair.of(SnapshotMetadata.newBuilder().setExists(false).build(),
                address);
          }
        })
        // filter out followers that do not have any snapshot or no updated snapshot
        .filter(pair -> pair.getLeft().getExists() && (localSnapshotInfo == null
            || localSnapshotInfo.getTermIndex().compareTo(toTermIndex(pair.getLeft())) < 0))
        .collect(Collectors.toList());
  }

  /**
   * Retrieves snapshot from the specified address.
   * @param snapshotMetadata helps identify which snapshot is desired
   * @param address where to retrieve it from
   * @return the index of the snapshot taken
   */
  private long downloadSnapshotFromAddress(SnapshotMetadata snapshotMetadata,
                                           InetSocketAddress address) {
    TermIndex index = toTermIndex(snapshotMetadata);
    LOG.info("Retrieving snapshot {} from {}", index, address);
    Instant start = Instant.now();
    RaftJournalServiceClient client = mClients.get(address);
    try {
      client.connect();
      Iterator<SnapshotData> it = client.requestLatestSnapshotData(snapshotMetadata);
      long totalBytesRead;
      long snapshotDiskSize;
      try (SnapshotGrpcInputStream stream = new SnapshotGrpcInputStream(it)) {
        DirectoryMarshaller marshaller = DirectoryMarshaller.Factory.create();
        snapshotDiskSize = marshaller.read(mStorage.getTmpDir().toPath(), stream);
        totalBytesRead = stream.totalBytes();
      }

      File finalSnapshotDestination = new File(mStorage.getSnapshotDir(),
          SimpleStateMachineStorage.getSnapshotFileName(snapshotMetadata.getSnapshotTerm(),
              snapshotMetadata.getSnapshotIndex()));
      FileUtils.moveDirectory(mStorage.getTmpDir(), finalSnapshotDestination);
      // update last duration and duration timer metrics
      mLastSnapshotDownloadDurationMs = Duration.between(start, Instant.now()).toMillis();
      MetricsSystem.timer(MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_DOWNLOAD_TIMER.getName())
              .update(mLastSnapshotDownloadDurationMs, TimeUnit.MILLISECONDS);
      LOG.info("Total milliseconds to download {}: {}", index, mLastSnapshotDownloadDurationMs);
      // update uncompressed snapshot size metric
      mLastSnapshotDownloadDiskSize = snapshotDiskSize;
      MetricsSystem.histogram(
              MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_DOWNLOAD_DISK_HISTOGRAM.getName())
          .update(mLastSnapshotDownloadDiskSize);
      LOG.info("Total extracted bytes of snapshot {}: {}", index, mLastSnapshotDownloadDiskSize);
      // update compressed snapshot size (aka size sent over the network)
      mLastSnapshotDownloadSize = totalBytesRead;
      MetricsSystem.histogram(
              MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_DOWNLOAD_HISTOGRAM.getName())
          .update(mLastSnapshotDownloadSize);
      LOG.info("Total bytes read from {} for {}: {}", address, index, mLastSnapshotDownloadSize);
      try (Timer.Context ctx = MetricsSystem.timer(
          MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_INSTALL_TIMER.getName()).time()) {
        mStorage.loadLatestSnapshot();
        mStorage.signalNewSnapshot();
      }
      LOG.info("Retrieved snapshot {} from {}", index, address);
      return snapshotMetadata.getSnapshotIndex();
    } catch (Exception e) {
      client.disconnect();
      LOG.warn("Failed to download snapshot {} from {}", index, address);
      LOG.debug("Download failure error", e);
      return RaftLog.INVALID_LOG_INDEX;
    } finally {
      FileUtils.deleteQuietly(mStorage.getTmpDir());
    }
  }

  @Override
  public void close() {
    mClients.values().forEach(AbstractClient::close);
  }

  private TermIndex toTermIndex(SnapshotMetadata metadata) {
    return TermIndex.valueOf(metadata.getSnapshotTerm(), metadata.getSnapshotIndex());
  }

  static class SnapshotGrpcInputStream extends InputStream {
    private final Iterator<SnapshotData> mIt;
    private long mTotalBytesRead = 0;
    // using a read-only ByteBuffer avoids array copy
    private ByteBuffer mCurrentBuffer = ByteBuffer.allocate(0);

    public SnapshotGrpcInputStream(Iterator<SnapshotData> iterator) {
      mIt = iterator;
    }

    @Override
    public int read() {
      if (!mCurrentBuffer.hasRemaining()) {
        if (!mIt.hasNext()) {
          return -1;
        }
        mCurrentBuffer = mIt.next().getChunk().asReadOnlyByteBuffer();
        LOG.debug("Received chunk of size {}: {}", mCurrentBuffer.capacity(), mCurrentBuffer);
        mTotalBytesRead += mCurrentBuffer.capacity();
      }
      return Byte.toUnsignedInt(mCurrentBuffer.get());
    }

    public long totalBytes() {
      return mTotalBytesRead;
    }
  }
}
