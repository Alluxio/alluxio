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
import alluxio.grpc.DownloadFilePRequest;
import alluxio.grpc.FileMetadata;
import alluxio.grpc.SnapshotData;
import alluxio.grpc.SnapshotMetadata;
import alluxio.master.selectionpolicy.MasterSelectionPolicy;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.amazonaws.annotation.GuardedBy;
import org.apache.commons.io.FileUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
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
  }

  /**
   * @return the log index of the last successful snapshot installation, or -1 if failure
   */
  public long downloadSnapshotFromFollowers() {
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
    List<InetSocketAddress> masterRpcAddresses =
        ConfigurationUtils.getMasterRpcAddresses(Configuration.global());
    InetSocketAddress localAddress = NetworkAddressUtils.getConnectAddress(
        NetworkAddressUtils.ServiceType.MASTER_RPC, Configuration.global());
    SnapshotInfo snapshotInfo = mStorage.getLatestSnapshot();
    if (snapshotInfo == null) {
      LOG.debug("No local snapshot found");
    } else {
      LOG.debug("Local snapshot is {}", TermIndex.valueOf(snapshotInfo.getTerm(),
          snapshotInfo.getIndex()));
    }

    List<Map.Entry<InetSocketAddress, SnapshotMetadata>> followerInfos =
        mClients.keySet().parallelStream()
        // filter ourselves out: we do not need to poll ourselves
        .filter(address -> !address.equals(localAddress))
        // map into a pair of (address, SnapshotMetadata) by requesting all followers in parallel
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
        .filter(entry -> snapshotInfo == null || (entry.getValue().getExists()
            && entry.getValue().getSnapshotIndex() > snapshotInfo.getIndex()))
        // sort them by snapshotIndex, the - sign is to reverse the sorting order
        .sorted(Comparator.comparingLong(entry -> -entry.getValue().getSnapshotIndex()))
        .collect(Collectors.toList());

    if (followerInfos.size() == 0) {
      LOG.debug("Did not find any follower with an updated snapshot");
      return RaftLog.INVALID_LOG_INDEX;
    }

    for (Map.Entry<InetSocketAddress, SnapshotMetadata> info : followerInfos) {
      InetSocketAddress address = info.getKey();
      SnapshotMetadata snapshotMetadata = info.getValue();
      try {
        RaftJournalServiceClient client = mClients.get(address);
        client.connect();
        List<FileMetadata> metadataList = snapshotMetadata.getFilesMetadataList();
        LOG.debug("Downloading {} snapshot files from {}", metadataList.size(), address);
        for (int i = 0; i < metadataList.size(); i++) {
          FileMetadata fileMetadata = metadataList.get(i);
          LOG.debug("Downloading snapshot file {} of {}: {}", i + 1, metadataList.size(),
              fileMetadata.getRelativePath());
          downloadFile(snapshotMetadata, fileMetadata, client);
          LOG.debug("Successfully downloaded snapshot file {}", fileMetadata.getRelativePath());
        }
        File finalSnapshotDestination = new File(mStorage.getSnapshotDir(),
            SimpleStateMachineStorage.getSnapshotFileName(snapshotMetadata.getSnapshotTerm(),
                snapshotMetadata.getSnapshotIndex()));
        FileUtils.moveDirectory(mStorage.getTmpDir(), finalSnapshotDestination);
        mStorage.loadLatestSnapshot();
        mStorage.signalNewSnapshot();
        LOG.debug("Finished snapshot download {}", snapshotMetadata.getSnapshotIndex());
        return snapshotMetadata.getSnapshotIndex();
      } catch (Exception e) {
        LOG.debug("Failed to download snapshot from {}", info.getKey());
      } finally {
        FileUtils.deleteQuietly(mStorage.getTmpDir());
      }
    }
    return RaftLog.INVALID_LOG_INDEX;
  }

  private void downloadFile(SnapshotMetadata snapshotMetadata, FileMetadata fileMetadata,
      RaftJournalServiceClient client) throws IOException {
    File file = new File(mStorage.getTmpDir(), fileMetadata.getRelativePath());
    file.getParentFile().mkdirs();
    file.createNewFile();

    DownloadFilePRequest request = DownloadFilePRequest.newBuilder()
        .setSnapshotTerm(snapshotMetadata.getSnapshotTerm())
        .setSnapshotIndex(snapshotMetadata.getSnapshotIndex())
        .setFileMetadata(fileMetadata)
        .build();
    Iterator<SnapshotData> data = client.downloadSnapshotFile(request);
    try (OutputStream out = Files.newOutputStream(file.toPath())) {
      while (data.hasNext()) {
        out.write(data.next().getChunk().toByteArray());
      }
    }
  }

  @Override
  public void close() {
    mClients.values().forEach(AbstractClient::close);
  }
}
