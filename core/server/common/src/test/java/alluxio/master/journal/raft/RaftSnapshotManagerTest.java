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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;

import org.apache.commons.io.FileUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RaftSnapshotManagerTest {
  private final List<GrpcServer> mGrpcServers = new ArrayList<>();
  private final List<TestRaftStorage> mStorages = new ArrayList<>();
  private final List<SnapshotDirStateMachineStorage> mSmStorages = new ArrayList<>();
  private final List<RaftSnapshotManager> mManagers = new ArrayList<>();

  @Before
  public void before() throws IOException {
    Configuration.set(PropertyKey.MASTER_JOURNAL_REQUEST_INFO_TIMEOUT, "10ms");
    // create Raft Storages and grpc servers for all masters
    // no need to create full master processes
    for (int i = 0; i < 3; i++) {
      // create the test storage
      TestRaftStorage raftStorage = new TestRaftStorage();
      mStorages.add(raftStorage);
      raftStorage.create();
      raftStorage.initialize();
      // create the state machine storage and initalize it using the raft storage
      SnapshotDirStateMachineStorage smStorage = new SnapshotDirStateMachineStorage();
      mSmStorages.add(smStorage);
      smStorage.init(raftStorage);
      // create and start a grpc server for each on a random available port
      GrpcServer server;
      try (ServerSocket socket = new ServerSocket(0)) {
        InetSocketAddress address = new InetSocketAddress(socket.getLocalPort());
        server = GrpcServerBuilder.forAddress(
            GrpcServerAddress.create(address.getHostName(), address),
                Configuration.global())
            .addService(ServiceType.RAFT_JOURNAL_SERVICE,
                new GrpcService(new RaftJournalServiceHandler(smStorage)))
            .build();
      }
      server.start();
      mGrpcServers.add(server);
    }
    // create snapshot managers based on the ports being used by the servers
    String hostAddress = InetAddress.getLocalHost().getHostAddress();
    String rpcAddresses = mGrpcServers.stream()
        .map(server -> String.format("%s:%d", hostAddress, server.getBindPort()))
        .collect(Collectors.joining(","));
    Configuration.set(PropertyKey.MASTER_RPC_ADDRESSES, rpcAddresses);
    // create SnapshotDownloaders after the fact: this is because the downloaders cache their
    // grpc clients to reuse them efficiently. They create the clients based on the configured
    // rpc addresses, excluding their own.
    for (int i = 0; i < mGrpcServers.size(); i++) {
      Configuration.set(PropertyKey.MASTER_RPC_PORT, mGrpcServers.get(i).getBindPort());
      mManagers.add(new RaftSnapshotManager(mSmStorages.get(i)));
    }
  }

  @After
  public void after() throws IOException {
    mGrpcServers.forEach(GrpcServer::shutdown);
    for (TestRaftStorage storage : mStorages) {
      storage.close();
    }
  }

  @Test
  public void noneAvailable() {
    mManagers.get(0).downloadSnapshotFromOtherMasters();
    long l = mManagers.get(0).waitForAttemptToComplete();
    Assert.assertEquals(RaftLog.INVALID_LOG_INDEX, l);
  }

  @Test
  public void simple() throws IOException {
    mStorages.get(1).createSnapshotFolder(1, 10);
    mSmStorages.get(1).loadLatestSnapshot();

    mManagers.get(0).downloadSnapshotFromOtherMasters();
    long l = mManagers.get(0).waitForAttemptToComplete();
    Assert.assertEquals(10, l);
    File snapshotDir1 = mSmStorages.get(1).getSnapshotDir();
    File snapshotDir0 = mSmStorages.get(0).getSnapshotDir();
    Assert.assertTrue(directoriesEqual(snapshotDir0, snapshotDir1));
  }

  @Test
  public void oneUnavailable() throws IOException {
    mGrpcServers.get(2).shutdown();
    mGrpcServers.get(2).awaitTermination();

    mStorages.get(1).createSnapshotFolder(1, 10);
    mSmStorages.get(1).loadLatestSnapshot();

    mManagers.get(0).downloadSnapshotFromOtherMasters();
    long l = mManagers.get(0).waitForAttemptToComplete();
    Assert.assertEquals(10, l);
    File snapshotDir1 = mSmStorages.get(1).getSnapshotDir();
    File snapshotDir0 = mSmStorages.get(0).getSnapshotDir();
    Assert.assertTrue(directoriesEqual(snapshotDir0, snapshotDir1));
  }

  @Test
  public void downloadHigherOne() throws IOException {
    mStorages.get(1).createSnapshotFolder(1, 10);
    mSmStorages.get(1).loadLatestSnapshot();
    mStorages.get(2).createSnapshotFolder(1, 100);
    mSmStorages.get(2).loadLatestSnapshot();

    mManagers.get(0).downloadSnapshotFromOtherMasters();
    long l = mManagers.get(0).waitForAttemptToComplete();
    Assert.assertEquals(100, l);
    File snapshotDir2 = mSmStorages.get(2).getSnapshotDir();
    File snapshotDir1 = mSmStorages.get(1).getSnapshotDir();
    File snapshotDir0 = mSmStorages.get(0).getSnapshotDir();
    Assert.assertTrue(directoriesEqual(snapshotDir0, snapshotDir2));
    Assert.assertFalse(directoriesEqual(snapshotDir1, snapshotDir0));
    Assert.assertFalse(directoriesEqual(snapshotDir1, snapshotDir2));
  }

  @Test
  public void higherOneUnavailable() throws IOException {
    mStorages.get(1).createSnapshotFolder(1, 10);
    mSmStorages.get(1).loadLatestSnapshot();
    mStorages.get(2).createSnapshotFolder(1, 100);
    mSmStorages.get(2).loadLatestSnapshot();
    mGrpcServers.get(2).shutdown();
    mGrpcServers.get(2).awaitTermination();

    mManagers.get(0).downloadSnapshotFromOtherMasters();
    long l = mManagers.get(0).waitForAttemptToComplete();
    Assert.assertEquals(10, l);
    File snapshotDir2 = mSmStorages.get(2).getSnapshotDir();
    File snapshotDir1 = mSmStorages.get(1).getSnapshotDir();
    File snapshotDir0 = mSmStorages.get(0).getSnapshotDir();
    Assert.assertTrue(directoriesEqual(snapshotDir0, snapshotDir1));
    Assert.assertFalse(directoriesEqual(snapshotDir2, snapshotDir0));
    Assert.assertFalse(directoriesEqual(snapshotDir2, snapshotDir1));
  }

  private boolean directoriesEqual(File dir1, File dir2) throws IOException {
    if (!dir1.getName().equals(dir2.getName())) {
      return false;
    }
    List<File> files1 = new ArrayList<>(FileUtils.listFiles(dir1, null, true));
    List<File> files2 = new ArrayList<>(FileUtils.listFiles(dir2, null, true));
    for (File file1 : files1) {
      Path relativize1 = dir1.toPath().relativize(file1.toPath());
      Optional<File> optionalFile = files2.stream()
          .filter(file -> dir2.toPath().relativize(file.toPath()).equals(relativize1))
          .findFirst();
      if (!optionalFile.isPresent() || !FileUtils.contentEquals(file1, optionalFile.get())) {
        return false;
      }
    }
    return true;
  }
}
