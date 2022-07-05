package alluxio.server.ft.journal.raft;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.QuorumServerState;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.MasterProcess;
import alluxio.master.TestUtils;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.raft.RaftJournalUtils;
import alluxio.multi.process.Master;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import org.apache.commons.io.FileUtils;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogCache;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class EmbeddedJournalIntegrationTestRatisInternal extends EmbeddedJournalIntegrationTestBase {
  AlluxioMasterProcess mLocalMaster;

  private static final int RESTART_TIMEOUT_MS = 6 * Constants.MINUTE_MS;
  private static final int NUM_MASTERS = 3;
  private static final int NUM_WORKERS = 0;

  @Test
  public void failover() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_FAILOVER)
            .setClusterName("EmbeddedJournalFaultTolerance_failover")
            .setNumMasters(NUM_MASTERS)
            .setNumWorkers(NUM_WORKERS)
            .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
            .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
            .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
            .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
            .build();
    mCluster.start();

    AlluxioURI testDir = new AlluxioURI("/dir");
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    mCluster.waitForAndKillPrimaryMaster(MASTER_INDEX_WAIT_TIME);
    assertTrue(fs.exists(testDir));
    mCluster.notifySuccess();
  }

  @Test
  public void startAMaster() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_SNAPSHOT_FOLLOWER)
            .setClusterName("EmbeddedJournalFaultTolerance_copySnapshotToFollower")
            .setNumMasters(3)
            .setNumWorkers(NUM_WORKERS)
            .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
            .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
            .addProperty(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, 1000)
            .addProperty(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "50KB")
            .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "3s")
            .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "6s")
            .addProperty(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, "5s")
            .build();
    mCluster.start();

    int catchUpMasterIndex = (mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME) + 1)
            % NUM_MASTERS;

    AlluxioURI testDir = new AlluxioURI("/dir");
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    for (int i = 0; i < 2000; i++) {
      fs.createDirectory(testDir.join("file" + i));
    }
    mCluster.getMetaMasterClient().checkpoint();
    mCluster.stopMaster(catchUpMasterIndex);
    File catchupJournalDir = new File(mCluster.getJournalDir(catchUpMasterIndex));
    FileUtils.deleteDirectory(catchupJournalDir);
    assertTrue(catchupJournalDir.mkdirs());
    mCluster.startMaster(catchUpMasterIndex);
    File raftDir = new File(RaftJournalUtils.getRaftJournalDir(catchupJournalDir),
            RaftJournalSystem.RAFT_GROUP_UUID.toString());
    waitForSnapshot(raftDir);
    System.out.println("Stopping master " + catchUpMasterIndex);
    mCluster.stopMaster(catchUpMasterIndex);

    // Report journal dirs to check
    for (int i = 0; i < 3; i++) {
      String journalPath = mCluster.getJournalDir(i);
      System.out.format("Master %s journal dir %s%n", i, journalPath);
    }

//    // Corrupt log for master 0
//    String masterJournal = mCluster.getJournalDir(catchUpMasterIndex);
//    Collection<String> allPaths = listFiles(masterJournal);
//    for (String p : allPaths) {
//      if (p.contains("log_inprogress_")) {
//        System.out.println("Found in progress log " + p);
//        String fileNewPath = p + ".moved";
//        boolean r = new File(p).renameTo(new File(fileNewPath));
//        System.out.println("Renamed log to " + fileNewPath + " result: " + r);
//      }
//    }
    // Validate current quorum size.
    waitForQuorumPropertySize(info -> info.getServerState() == QuorumServerState.AVAILABLE, 2);
    System.out.println(mCluster.getJournalMasterClientForMaster().getQuorumInfo());
//    assertEquals(2,
//        mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList().size());

    // Can this master restart?
    // TODO(jiacheng): where is the failure log?
//    mCluster.startMaster(0);


    // Use the target master and start in this process
    Master m = mCluster.getMasters().get(catchUpMasterIndex);
    System.out.format("Master %d has config: %s%n", catchUpMasterIndex, m.getConf());

    // Start a local master
    startLocalMaster(m);

    System.out.println("Continue testing logic");
    // What is the status of the master?
    assertTrue(fs.exists(testDir));

    // How to wait for the master to be ready?
    System.out.println("Waiting on the quorum to become 3");
    waitForQuorumPropertySizeLong(info -> info.getServerState() == QuorumServerState.AVAILABLE, 3);
    System.out.println("3 masters in the quorum");
    System.out.println(mCluster.getJournalMasterClientForMaster().getQuorumInfo());

    corruptRatisServerInternally();
    // TODO(jiacheng): what now?
    System.out.println("Create files and wait for the standby to die");
    for (int i = 0; i < 10000; i++) {
      fs.createFile(new AlluxioURI("/test"+i)).close();
      if (i % 100 == 0) {
        System.out.format("%d files%n", i);
      }
     }
    System.out.println("10000 files created");
    System.out.println(fs.getStatus(new AlluxioURI("/test10000")));
    System.out.println("Now the quorum status is " + mCluster.getJournalMasterClientForMaster().getQuorumInfo());

    waitForQuorumPropertySize(info -> info.getServerState() == QuorumServerState.AVAILABLE, 2);
    System.out.println("The standby has died");

    mCluster.notifySuccess();
  }

  private Collection<String> listFiles(String path) throws Exception {
    System.out.println("List " + path);
    try (Stream<Path> stream = Files.walk(Paths.get(path), 4)) {
      List<String> allPaths =  stream.map(Path::toAbsolutePath).map(Path::toString).collect(Collectors.toList());
      System.out.println(allPaths);
      return allPaths;
    }
  }

  private void corruptRatisServerInternally() throws Exception {
    // Go to the standby master and kill the ratis server
    RaftServer s = getRaftServer();
//    System.out.println("Manually shutdown the RaftServer");
//    s.close();

    Class c = s.getClass();
    Method[] allMethods = c.getDeclaredMethods();
    Method target = null;
    for (Method mm : allMethods) {
      if (mm.getName().contains("getImpls")) {
        System.out.println("Method is " + mm);
        target = mm;
      }
    }
    target.setAccessible(true);
    Object implObj = target.invoke(s);
    List<RaftServer.Division> impls = (List<RaftServer.Division>) implObj;
    RaftServer.Division d = impls.get(0);
    SegmentedRaftLog log = (SegmentedRaftLog) d.getRaftLog();
    Field f = log.getClass().getDeclaredField("cache");
    f.setAccessible(true);
    SegmentedRaftLogCache cache = (SegmentedRaftLogCache) f.get(log);
    Field f2 = cache.getClass().getDeclaredField("openSegment");
    f2.setAccessible(true);
    f2.set(cache, null);
    System.out.println("Server is corrupted internally");
    // This should throw an NPE
//    cache.getTotalCacheSize();
  }

  private RaftServer getRaftServer() throws Exception {
    // Create Field object
    Field privateField
            = MasterProcess.class.getDeclaredField("mJournalSystem");
    // Set the accessibility as true
    privateField.setAccessible(true);

    // Store the value of private field in variable
    RaftJournalSystem journalSystem = (RaftJournalSystem) privateField.get(mLocalMaster);
    Field pf2 = RaftJournalSystem.class.getDeclaredField("mServer");
    pf2.setAccessible(true);
    RaftServer server = (RaftServer) pf2.get(journalSystem);



    return server;
  }



  private void waitForSnapshot(File raftDir) throws InterruptedException, TimeoutException {
    File snapshotDir = new File(raftDir, "sm");
    final int RETRY_INTERVAL_MS = 200; // milliseconds
    CommonUtils.waitFor("snapshot is downloaded", () -> {
      File[] files = snapshotDir.listFiles();
      return files != null && files.length > 1 && files[0].length() > 0;
    }, WaitForOptions.defaults().setInterval(RETRY_INTERVAL_MS).setTimeoutMs(RESTART_TIMEOUT_MS));
  }


  public void startLocalMaster(Master m) throws Exception {
    Map<PropertyKey, Object> config = m.getConf();
    System.out.println("Starting a local master from past master configurations " + config);

    // The journal files are still locked mysteriously, need to copy them
    String pastJournalFolder = (String) config.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    String newPath = pastJournalFolder + "_new";
    System.out.format("Moving the past journals from %s to %s%n", pastJournalFolder, newPath);
    new File(pastJournalFolder).renameTo(new File(newPath));
    System.out.println("Journals moved");
    Configuration.set(PropertyKey.MASTER_JOURNAL_FOLDER, newPath);
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT, config.get(PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT));
    Configuration.set(PropertyKey.CONF_DIR, config.get(PropertyKey.CONF_DIR));
    Configuration.set(PropertyKey.MASTER_METASTORE_DIR, config.get(PropertyKey.MASTER_METASTORE_DIR));
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "process-local-test-master");
    Configuration.set(PropertyKey.MASTER_WEB_PORT, config.get(PropertyKey.MASTER_WEB_PORT));
    Configuration.set(PropertyKey.MASTER_RPC_PORT, config.get(PropertyKey.MASTER_RPC_PORT));
    Configuration.set(PropertyKey.LOGGER_TYPE, config.get(PropertyKey.LOGGER_TYPE));
    Configuration.set(PropertyKey.LOGS_DIR, config.get(PropertyKey.LOGS_DIR));
    System.out.println("Logs in " + config.get(PropertyKey.LOGS_DIR));

    mLocalMaster = AlluxioMasterProcess.Factory.create();

    Runnable runMaster = () -> {
      try {
        mLocalMaster.start();
      } catch (InterruptedException e) {
        // this is expected
      } catch (Exception e) {
        // Log the exception as the RuntimeException will be caught and handled silently by
        // JUnit
        System.out.println("Met exception starting master " + e.getMessage());
        e.printStackTrace();
        throw new RuntimeException(e + " \n Start Worker Error \n" + e.getMessage(), e);
      }
    };
    Thread thread = new Thread(runMaster);
    thread.setName("MasterThread-" + System.identityHashCode(thread));
    thread.start();
    System.out.println("Started local master");

    TestUtils.waitForReady(mLocalMaster);
    System.out.println("Master is now ready");
  }
}
