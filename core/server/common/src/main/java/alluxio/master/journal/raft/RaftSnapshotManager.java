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
import alluxio.concurrent.jsr.CompletableFuture;
import alluxio.conf.Configuration;
import alluxio.grpc.SnapshotData;
import alluxio.grpc.SnapshotMetadata;
import alluxio.master.selectionpolicy.MasterSelectionPolicy;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.TarUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;

import com.amazonaws.annotation.GuardedBy;
import com.codahale.metrics.Timer;
import org.apache.commons.io.FileUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Manages a snapshot download.
 */
public class RaftSnapshotManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RaftSnapshotManager.class);
  private long mLastSnapshotDownloadDuration = -1;
  private long mLastSnapshotDownloadSize = -1;

  private final SnapshotDirStateMachineStorage mStorage;
  private final Map<InetSocketAddress, RaftJournalServiceClient> mClients;

  @Nullable @GuardedBy("this")
  private CompletableFuture<Long> mDownloadFuture = null;

  RaftSnapshotManager(SnapshotDirStateMachineStorage storage) {
    mStorage = storage;

    InetSocketAddress localAddress = NetworkAddressUtils.getConnectAddress(
        NetworkAddressUtils.ServiceType.MASTER_RPC, Configuration.global());
    mClients = ConfigurationUtils.getMasterRpcAddresses(Configuration.global()).stream()
        .filter(address -> !address.equals(localAddress))
        .collect(Collectors.toMap(Function.identity(), address -> {
          MasterSelectionPolicy policy = MasterSelectionPolicy.Factory.specifiedMaster(address);
          return new RaftJournalServiceClient(policy);
        }));

    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EMBEDDED_JOURNAL_LAST_SNAPSHOT_DOWNLOAD_DURATION.getName(),
        () -> mLastSnapshotDownloadDuration);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EMBEDDED_JOURNAL_LAST_SNAPSHOT_DOWNLOAD_SIZE.getName(),
        () -> mLastSnapshotDownloadSize);
  }

  /**
   * @return the log index of the last successful snapshot installation, or -1 if failure
   */
  public long downloadSnapshotFromOtherMasters() {
    if (mDownloadFuture == null) {
      mDownloadFuture = CompletableFuture.supplyAsync(this::core).exceptionally(err -> {
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
      LOG.debug("No local snapshot found");
    } else {
      LOG.debug("Local snapshot is {}", TermIndex.valueOf(localSnapshotInfo.getTerm(),
          localSnapshotInfo.getIndex()));
    }
    List<Map.Entry<InetSocketAddress, SnapshotMetadata>> followerInfos;
    try {
      followerInfos = CommonUtils.waitForResult("Snapshot info to be retrieved",
          () -> retrieveFollowerInfos(localSnapshotInfo),
          list -> list.size() > 0,
          WaitForOptions.defaults().setInterval(10_000).setTimeoutMs(60_000));
    } catch (Exception e) {
      LOG.debug("Did not find any follower with an updated snapshot");
      return RaftLog.INVALID_LOG_INDEX;
    }

    for (Map.Entry<InetSocketAddress, SnapshotMetadata> info : followerInfos) {
      InetSocketAddress address = info.getKey();
      SnapshotMetadata snapshotMetadata = info.getValue();
      Timer.Context ctx = MetricsSystem
          .timer(MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_DOWNLOAD_TIMER.getName()).time();
      long index;
      if ((index = downloadSnapshotFromAddress(snapshotMetadata, address))
          != RaftLog.INVALID_LOG_INDEX) {
        mLastSnapshotDownloadDuration = ctx.stop() / 1_000_000; // get time in ms
        return index;
      }
    }
    return RaftLog.INVALID_LOG_INDEX;
  }

  /**
   * @param localSnapshotInfo contains information about the most up-to-date snapshot on this master
   * @return a sorted list of pairs containing a follower's address and its most up-to-date snapshot
   */
  private List<Map.Entry<InetSocketAddress, SnapshotMetadata>> retrieveFollowerInfos(
      SnapshotInfo localSnapshotInfo) {
    return mClients.keySet().parallelStream()
        // map to a pair of (address, SnapshotMetadata) by requesting all followers in parallel
        .collect(Collectors.toMap(Function.identity(), address -> {
          LOG.debug("Requesting snapshot info from {}", address);
          try {
            RaftJournalServiceClient client = mClients.get(address);
            client.connect();
            SnapshotMetadata metadata = client.requestLatestSnapshotInfo();
            LOG.debug("Received snapshot info from {} with status {}", address,
                TermIndex.valueOf(metadata.getSnapshotTerm(), metadata.getSnapshotIndex()));
            return metadata;
          } catch (Exception e) {
            LOG.debug("Failed to retrieve snapshot info from {}", address, e);
            return SnapshotMetadata.newBuilder().setExists(false).build();
          }
        })).entrySet().stream()
        // filter out followers that do not have any snapshot or no updated snapshot
        .filter(entry -> entry.getValue().getExists() && (localSnapshotInfo == null
            || entry.getValue().getSnapshotIndex() > localSnapshotInfo.getIndex()))
        // sort them by snapshotIndex, the - sign is to reverse the sorting order
        .sorted(Comparator.comparingLong(entry -> -entry.getValue().getSnapshotIndex()))
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
    TermIndex termIndex = TermIndex.valueOf(snapshotMetadata.getSnapshotTerm(),
        snapshotMetadata.getSnapshotIndex());
    LOG.debug("Retrieving snapshot {} from {}", termIndex, address);
    try {
      RaftJournalServiceClient client = mClients.get(address);
      client.connect();
      Iterator<SnapshotData> it = client.requestLatestSnapshotData(snapshotMetadata);
      try (InputStream snapshotInStream = new InputStream() {
        long mTotalBytesRead = 0L;
        ByteBuffer mCurrentBuffer = null; // using a read-only ByteBuffer avoids array copy

        @Override
        public int read() {
          if (mCurrentBuffer == null || !mCurrentBuffer.hasRemaining()) {
            if (!it.hasNext()) {
              return -1;
            }
            mCurrentBuffer = it.next().getChunk().asReadOnlyByteBuffer();
            LOG.debug("Received chunk of size {}: {}", mCurrentBuffer.capacity(), mCurrentBuffer);
            mTotalBytesRead += mCurrentBuffer.capacity();
          }
          return Byte.toUnsignedInt(mCurrentBuffer.get());
        }

        @Override
        public void close() {
          LOG.debug("Total bytes read from {}: {}", address, mTotalBytesRead);
          MetricsSystem.histogram(
              MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_DOWNLOAD_HISTOGRAM.getName())
              .update(mTotalBytesRead);
          mLastSnapshotDownloadSize = mTotalBytesRead;
        }
      }) {
        TarUtils.readTarGz(mStorage.getTmpDir().toPath(), snapshotInStream);
      }

      File finalSnapshotDestination = new File(mStorage.getSnapshotDir(),
          SimpleStateMachineStorage.getSnapshotFileName(snapshotMetadata.getSnapshotTerm(),
              snapshotMetadata.getSnapshotIndex()));
      FileUtils.moveDirectory(mStorage.getTmpDir(), finalSnapshotDestination);
      mStorage.loadLatestSnapshot();
      mStorage.signalNewSnapshot();
      LOG.debug("Retrieved snapshot {} from {}", termIndex, address);
      return snapshotMetadata.getSnapshotIndex();
    } catch (IOException e) {
      LOG.debug("Failed to download snapshot {} from {}", termIndex, address);
      return RaftLog.INVALID_LOG_INDEX;
    } finally {
      FileUtils.deleteQuietly(mStorage.getTmpDir());
    }
  }

  @Override
  public void close() {
    mClients.values().forEach(AbstractClient::close);
  }
}
