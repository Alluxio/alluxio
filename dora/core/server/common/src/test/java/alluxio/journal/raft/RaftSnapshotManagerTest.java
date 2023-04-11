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

import net.bytebuddy.utility.RandomString;
import org.apache.commons.io.FileUtils;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageImpl;
import org.apache.ratis.server.storage.StorageImplUtils;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class RaftSnapshotManagerTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private final List<GrpcServer> mGrpcServers = new ArrayList<>();
  private final List<SnapshotDirStateMachineStorage> mSmStorages = new ArrayList<>();
  private final List<RaftSnapshotManager> mManagers = new ArrayList<>();

  @Before
  public void before() throws IOException {
    Configuration.set(PropertyKey.MASTER_JOURNAL_REQUEST_INFO_TIMEOUT, "10ms");
    // create Raft Storages and grpc servers for all masters
    // no need to create full master processes
    for (int i = 0; i < 3; i++) {
      // create the state machine storage and initalize it using the raft storage
      SnapshotDirStateMachineStorage smStorage = createStateMachineStorage(mFolder);
      mSmStorages.add(smStorage);
      RaftJournalServiceHandler handler = new RaftJournalServiceHandler(smStorage);
      // create and start a grpc server for each on a random available port
      GrpcServer server = createGrpcServer(handler);
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
      mManagers.add(new RaftSnapshotManager(mSmStorages.get(i),
          Executors.newSingleThreadExecutor()));
    }
  }

  @After
  public void after() throws IOException {
    mGrpcServers.forEach(GrpcServer::shutdown);
    mGrpcServers.forEach(GrpcServer::awaitTermination);
  }

  @Test
  public void noneAvailable() {
    mManagers.get(0).downloadSnapshotFromOtherMasters();
    long l = mManagers.get(0).waitForAttemptToComplete();
    Assert.assertEquals(RaftLog.INVALID_LOG_INDEX, l);
  }

  @Test
  public void simple() throws IOException {
    createSampleSnapshot(mSmStorages.get(1), 1, 10);
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

    createSampleSnapshot(mSmStorages.get(1), 1, 10);
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
    createSampleSnapshot(mSmStorages.get(1), 1, 10);
    mSmStorages.get(1).loadLatestSnapshot();
    createSampleSnapshot(mSmStorages.get(2), 1, 100);
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
    createSampleSnapshot(mSmStorages.get(1), 1, 10);
    createSampleSnapshot(mSmStorages.get(2), 1, 100);
    mSmStorages.get(1).loadLatestSnapshot();
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

  @Test
  public void successThenFailureThenSuccess() throws IOException {
    // eliminate one of the two servers
    mGrpcServers.get(2).shutdown();
    mGrpcServers.get(2).awaitTermination();

    createSampleSnapshot(mSmStorages.get(1), 1, 10);
    mSmStorages.get(1).loadLatestSnapshot();
    mManagers.get(0).downloadSnapshotFromOtherMasters();
    long l = mManagers.get(0).waitForAttemptToComplete();
    Assert.assertEquals(10, l);
    File snapshotDir1 = mSmStorages.get(1).getSnapshotDir();
    File snapshotDir0 = mSmStorages.get(0).getSnapshotDir();
    Assert.assertTrue(directoriesEqual(snapshotDir0, snapshotDir1));

    createSampleSnapshot(mSmStorages.get(1), 2, 100);
    mSmStorages.get(1).loadLatestSnapshot();
    int bindPort = mGrpcServers.get(1).getBindPort();
    mGrpcServers.get(1).shutdown();
    mGrpcServers.get(1).awaitTermination();
    mManagers.get(0).downloadSnapshotFromOtherMasters();
    l = mManagers.get(0).waitForAttemptToComplete();
    Assert.assertEquals(-1, l); // failure expected

    // recreate grpc server on the same port
    mGrpcServers.add(1,
        createGrpcServer(new RaftJournalServiceHandler(mSmStorages.get(1)), bindPort));
    mGrpcServers.get(1).start();
    createSampleSnapshot(mSmStorages.get(1), 3, 1_000);
    mSmStorages.get(1).loadLatestSnapshot();
    mManagers.get(0).downloadSnapshotFromOtherMasters();
    l = mManagers.get(0).waitForAttemptToComplete();
    Assert.assertEquals(1_000, l);
    // server 1 has more snapshots than server 0
    Assert.assertFalse(directoriesEqual(snapshotDir0, snapshotDir1));
  }

  public static SnapshotDirStateMachineStorage createStateMachineStorage(TemporaryFolder folder)
      throws IOException {
    RaftStorageImpl raftStorage = StorageImplUtils.newRaftStorage(folder.newFolder(),
        RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION, RaftStorage.StartupOption.RECOVER,
        RaftServerConfigKeys.STORAGE_FREE_SPACE_MIN_DEFAULT.getSize());
    raftStorage.initialize();
    SnapshotDirStateMachineStorage smStorage = new SnapshotDirStateMachineStorage();
    smStorage.init(raftStorage);
    return smStorage;
  }

  public static GrpcServer createGrpcServer(RaftJournalServiceHandler handler) throws IOException {
    return createGrpcServer(handler, 0);
  }

  public static GrpcServer createGrpcServer(RaftJournalServiceHandler handler, int port)
      throws IOException {
    try (ServerSocket socket = new ServerSocket(port)) {
      InetSocketAddress address = new InetSocketAddress(socket.getLocalPort());
      return GrpcServerBuilder.forAddress(
              GrpcServerAddress.create(address.getHostName(), address),
              Configuration.global())
          .addService(ServiceType.RAFT_JOURNAL_SERVICE, new GrpcService(handler))
          .build();
    }
  }

  public static void createSampleSnapshot(StateMachineStorage smStorage, long term, long index)
      throws IOException {
    String snapshotDirName = SimpleStateMachineStorage.getSnapshotFileName(term, index);
    File dir = new File(smStorage.getSnapshotDir(), snapshotDirName);
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException(String.format("Unable to create directory %s", dir));
    }
    for (int i = 0; i < 10; i++) {
      String s = "dummy-file-" + i;
      File file = new File(dir, s);
      try (FileOutputStream outputStream = new FileOutputStream(file)) {
        outputStream.write(RandomString.make().getBytes());
      }
      MD5Hash md5Hash = MD5FileUtil.computeMd5ForFile(file);
      MD5FileUtil.saveMD5File(file, md5Hash);
    }
  }

  public static boolean directoriesEqual(File dir1, File dir2) throws IOException {
    if (!dir1.getName().equals(dir2.getName())) {
      return false;
    }
    List<File> files1 = new ArrayList<>(FileUtils.listFiles(dir1, null, true));
    List<File> files2 = new ArrayList<>(FileUtils.listFiles(dir2, null, true));
    if (files1.size() != files2.size()) {
      return false;
    }
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
