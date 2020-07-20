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

package alluxio.cli.bundler.command;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.cli.bundler.InfoCollectorTestUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.cglib.core.Local;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CollectLogCommandTest {
  private InstancedConfiguration mConf;
  private File mTestDir;
  private Set<String> mExpectedFiles;

  @Before
  public void initLogDirAndConf() throws IOException {
    mTestDir = prepareLogDir();
    mConf = InstancedConfiguration.defaults();
    mConf.set(PropertyKey.LOGS_DIR, mTestDir.getAbsolutePath());
  }

  @After
  public void emptyLogDir() {
    mConf.unset(PropertyKey.LOGS_DIR);
    mTestDir.delete();
  }

  // Prepare a temp dir with some log files
  private File prepareLogDir() throws IOException {
    // The dir path will contain randomness so will be different every time
    File testLogDir = InfoCollectorTestUtils.createTemporaryDirectory();
    // Prepare the normal log files that normal users will have
    for (String s : CollectLogCommand.FILE_NAMES) {
      InfoCollectorTestUtils.createFileInDir(testLogDir, s);
    }
    // Create some extra log files
    InfoCollectorTestUtils.createFileInDir(testLogDir, "master.log.1");
    InfoCollectorTestUtils.createFileInDir(testLogDir, "master.log.2");
    InfoCollectorTestUtils.createFileInDir(testLogDir, "worker.log.backup");
    // Remove the user file and create a directory
    File userDir = new File(testLogDir, "user");
    if (userDir.exists()) {
      userDir.delete();
    }
    // Put logs in the user log dir
    File userLogDir = InfoCollectorTestUtils.createDirInDir(testLogDir, "user");
    InfoCollectorTestUtils.createFileInDir(userLogDir, "user_hadoop.log");
    InfoCollectorTestUtils.createFileInDir(userLogDir, "user_hadoop.out");
    InfoCollectorTestUtils.createFileInDir(userLogDir, "user_root.log");
    InfoCollectorTestUtils.createFileInDir(userLogDir, "user_root.out");

    // Set up expectation
    Set<String> createdFiles = getAllFileNamesRelative(testLogDir, testLogDir);
    mExpectedFiles = createdFiles;

    return testLogDir;
  }

  @Test
  public void logFilesCopied() throws IOException, AlluxioException {
    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));

    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());

    verifyAllFiles(subDir);
  }

  private void verifyAllFiles(File targetDir) throws IOException {
    Set<String> copiedFiles = getAllFileNamesRelative(targetDir, targetDir);

    System.out.println("Expected");
    System.out.println(mExpectedFiles);
    System.out.println("Copied");
    System.out.println(copiedFiles);
    Set<String> missing = new HashSet<>(mExpectedFiles);
    missing.removeAll(copiedFiles);
    System.out.println("Missing:");
    System.out.println(missing);
    Set<String> extra = new HashSet<>(copiedFiles);
    extra.removeAll(mExpectedFiles);
    System.out.println("Extra:");
    System.out.println(extra);
    assertTrue(mExpectedFiles.containsAll(copiedFiles));
    assertTrue(copiedFiles.containsAll(mExpectedFiles));
  }

  private Set<String> getAllFileNamesRelative(File dir, File baseDir) throws IOException {
    if (!dir.isDirectory()) {
      throw new IOException(String.format("Expected a directory but found a file at %s%n", dir.getCanonicalPath()));
    }
    Set<String> fileSet = new HashSet<>();
    List<File> allFiles = recursiveListDir(dir);
    for (File f : allFiles) {
      String relativePath = baseDir.toURI().relativize(f.toURI()).toString();
      fileSet.add(relativePath);
    }
    return fileSet;
  }

  private List<File> recursiveListDir(File dir) {
    File[] files = dir.listFiles();
    List<File> result = new ArrayList<>(files.length);
    for (File f : files) {
      if (f.isDirectory()) {
        result.addAll(recursiveListDir(f));
        continue;
      }
      result.add(f);
    }
    return result;
  }


  @Test
  public void irrelevantFileIgnored() throws Exception {
    // This file will not be copied
    // Not included in the expected set
    InfoCollectorTestUtils.createFileInDir(mTestDir, "irrelevant");

    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());

    verifyAllFiles(subDir);
  }

  @Test
  public void fileNameExcluded() throws Exception {
    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{
            targetDir.getAbsolutePath()
    };
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(eq("exclude-logs"))).thenReturn(true);
    when(mockCommandLine.getOptionValue(eq("exclude-logs"))).thenReturn("master.log.1, worker");
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());
    mExpectedFiles.remove("master.log.1");
    mExpectedFiles.remove("worker.log");
    mExpectedFiles.remove("worker.out");
    mExpectedFiles.remove("worker.log.backup");

    verifyAllFiles(subDir);
  }

  @Test
  public void fileNameIncluded() throws Exception {
    InfoCollectorTestUtils.createFileInDir(mTestDir, "alluxio_gc.log");
    InfoCollectorTestUtils.createFileInDir(mTestDir, "alluxio_gc.log.1");
    InfoCollectorTestUtils.createFileInDir(mTestDir, "alluxio_gc.log.2");

    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{
            targetDir.getAbsolutePath()
    };
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(eq("include-logs"))).thenReturn(true);
    when(mockCommandLine.getOptionValue(eq("include-logs"))).thenReturn("alluxio_gc");
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());
    mExpectedFiles.add("alluxio_gc.log");
    mExpectedFiles.add("alluxio_gc.log.1");
    mExpectedFiles.add("alluxio_gc.log.2");

    verifyAllFiles(subDir);
  }

  @Test
  public void endTimeFilter() throws Exception {
    // Define an issue end datetime
    // We ignore logs that are created after this
    LocalDateTime issueEnd = LocalDateTime.of(2020, 7, 8, 18, 0, 0);
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Other logs all end before this issue
    LocalDateTime logEnd = issueEnd.minusHours(1);
    long logEndTimestamp = logEnd.toEpochSecond(ZoneOffset.UTC);
    for (File f : recursiveListDir(mTestDir)) {
      f.setLastModified(logEndTimestamp);
    }

    // Some logs are created after this time, should be ignored
    File masterLog = new File(mTestDir, "master.log");
    String log = "2020-07-08 18:53:45,129 INFO  CopycatServer - Server started successfully!\n" +
            "2020-07-08 18:53:59,129 INFO  RaftJournalSystem - Started Raft Journal System in 13175ms\n" +
            "2020-07-09 00:01:59,135 INFO  DefaultMetaMaster - Standby master with address localhost:19998 starts sending heartbeat to leader master.\n" +
            "2020-07-09 00:03:59,135 INFO  AlluxioMasterProcess - All masters started\n" +
            "2020-07-09 01:12:59,138 INFO  AbstractPrimarySelector - Primary selector transitioning to PRIMARY\n" +
            "2020-07-09 01:53:59,139 INFO  AbstractMaster - TableMaster: Stopped secondary master.";
    writeToFile(masterLog, log);
    masterLog.setLastModified(LocalDateTime.of(2020, 7, 9, 1, 53, 59).toEpochSecond(ZoneOffset.UTC));

    // Some logs overlap with the end time
    File masterLog1 = new File(mTestDir, "master.log.1");
    String log1 = "2020-07-08 16:53:44,639 INFO  BlockMasterFactory - Creating alluxio.master.block.BlockMaster \n" +
            "2020-07-08 16:53:44,639 INFO  TableMasterFactory - Creating alluxio.master.table.TableMaster \n" +
            "2020-07-08 17:01:44,639 INFO  MetaMasterFactory - Creating alluxio.master.meta.MetaMaster \n" +
            "2020-07-08 17:01:45,639 INFO  FileSystemMasterFactory - Creating alluxio.master.file.FileSystemMaster \n" +
            "2020-07-08 18:53:44,751 INFO  UnderDatabaseRegistry - Registered UDBs: hive,glue\n" +
            "2020-07-08 18:53:44,756 INFO  LayoutRegistry - Registered Table Layouts: hive";
    writeToFile(masterLog1, log1);
    masterLog1.setLastModified(LocalDateTime.of(2020, 7, 8, 18, 53, 45).toEpochSecond(ZoneOffset.UTC));

    // Some logs end before this end time
    File masterLog2 = new File(mTestDir, "master.log.2");
    String log2 = "2020-07-08 15:40:45,656 INFO  HdfsUnderFileSystem - Successfully instantiated SupportedHdfsActiveSyncProvider\n" +
            "2020-07-08 15:51:45,782 INFO  RocksStore - Opened rocks database under path /data/02/alluxio/metastore/inodes\n" +
            "2020-07-08 15:53:45,862 INFO  RocksStore - Opened rocks database under path /data/02/alluxio/metastore/blocks\n" +
            "2020-07-08 16:52:41,876 INFO  RocksStore - Opened rocks database under path /data/02/alluxio/metastore/inodes\n" +
            "2020-07-08 16:53:43,876 INFO  JournalStateMachine - Initialized new journal state machine";
    writeToFile(masterLog2, log2);
    masterLog1.setLastModified(LocalDateTime.of(2020, 7, 8, 16, 53, 44).toEpochSecond(ZoneOffset.UTC));

    // Copy
    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{
            targetDir.getAbsolutePath()
    };
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(eq("end-time"))).thenReturn(true);
    when(mockCommandLine.getOptionValue(eq("end-time"))).thenReturn(issueEnd.format(fmt));
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());
    mExpectedFiles.remove("master.log");
    verifyAllFiles(subDir);
  }

  private void writeToFile(File f, String content) throws IOException {
    try (FileWriter writer = new FileWriter(f)) {
      writer.write(content);
    }
  }

  @Test
  public void startTimeFilter() throws Exception {
    // Define an issue start datetime
    // We ignore logs that are done before this
    LocalDateTime issueEnd = LocalDateTime.of(2020, 7, 8, 18, 0, 0);
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Other logs all end before this issue
    LocalDateTime logEnd = issueEnd.minusHours(1);
    long logEndTimestamp = logEnd.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    for (File f : recursiveListDir(mTestDir)) {
      f.setLastModified(logEndTimestamp);
    }

    // Some logs are created after this time, should be included
    File masterLog = new File(mTestDir, "master.log");
    String log = "2020-07-08 18:53:45,129 INFO  CopycatServer - Server started successfully!\n" +
            "2020-07-08 18:53:59,129 INFO  RaftJournalSystem - Started Raft Journal System in 13175ms\n" +
            "2020-07-09 00:01:59,135 INFO  DefaultMetaMaster - Standby master with address localhost:19998 starts sending heartbeat to leader master.\n" +
            "2020-07-09 00:03:59,135 INFO  AlluxioMasterProcess - All masters started\n" +
            "2020-07-09 01:12:59,138 INFO  AbstractPrimarySelector - Primary selector transitioning to PRIMARY\n" +
            "2020-07-09 01:53:59,139 INFO  AbstractMaster - TableMaster: Stopped secondary master.";
    writeToFile(masterLog, log);
    masterLog.setLastModified(LocalDateTime.of(2020, 7, 9, 1, 53, 59).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

    // Some logs overlap with the start time, should be included
    File masterLog1 = new File(mTestDir, "master.log.1");
    String log1 = "2020-07-08 16:53:44,639 INFO  BlockMasterFactory - Creating alluxio.master.block.BlockMaster \n" +
            "2020-07-08 16:53:44,639 INFO  TableMasterFactory - Creating alluxio.master.table.TableMaster \n" +
            "2020-07-08 17:01:44,639 INFO  MetaMasterFactory - Creating alluxio.master.meta.MetaMaster \n" +
            "2020-07-08 17:01:45,639 INFO  FileSystemMasterFactory - Creating alluxio.master.file.FileSystemMaster \n" +
            "2020-07-08 18:53:44,751 INFO  UnderDatabaseRegistry - Registered UDBs: hive,glue\n" +
            "2020-07-08 18:53:44,756 INFO  LayoutRegistry - Registered Table Layouts: hive";
    writeToFile(masterLog1, log1);
    masterLog1.setLastModified(LocalDateTime.of(2020, 7, 8, 18, 53, 45).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

    // Some logs end before the start time, should be ignored
    File masterLog2 = new File(mTestDir, "master.log.2");
    String log2 = "2020-07-08 15:40:45,656 INFO  HdfsUnderFileSystem - Successfully instantiated SupportedHdfsActiveSyncProvider\n" +
            "2020-07-08 15:51:45,782 INFO  RocksStore - Opened rocks database under path /data/02/alluxio/metastore/inodes\n" +
            "2020-07-08 15:53:45,862 INFO  RocksStore - Opened rocks database under path /data/02/alluxio/metastore/blocks\n" +
            "2020-07-08 16:52:41,876 INFO  RocksStore - Opened rocks database under path /data/02/alluxio/metastore/inodes\n" +
            "2020-07-08 16:53:43,876 INFO  JournalStateMachine - Initialized new journal state machine";
    writeToFile(masterLog2, log2);
    masterLog2.setLastModified(LocalDateTime.of(2020, 7, 8, 16, 53, 44).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

    // Copy
    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{
            targetDir.getAbsolutePath()
    };
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(eq("start-time"))).thenReturn(true);
    when(mockCommandLine.getOptionValue(eq("start-time"))).thenReturn(issueEnd.format(fmt));
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());
    mExpectedFiles = new HashSet<>();
    mExpectedFiles.add("master.log");
    mExpectedFiles.add("master.log.1");
    verifyAllFiles(subDir);
  }

  class DatetimeMatcher extends TypeSafeMatcher<LocalDateTime> {
    private LocalDateTime datetime;

    public DatetimeMatcher setDatetime(LocalDateTime datetime) {
      this.datetime = datetime;
      return this;
    }

    @Override
    protected boolean matchesSafely(LocalDateTime s) {
      return datetime.isEqual(s);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("only digits");
    }
  }

  @Test
  public void inferDateFromLog() throws Exception {
    // Yarn application log default format
    String yarnAppLog = "\n" +
            "Logged in as: user\n" +
            "Application\n" +
            "About\n" +
            "Jobs\n" +
            "Tools\n" +
            "Log Type: container-localizer-syslog\n" +
            "\n" +
            "Log Upload Time: Mon May 18 16:11:22 +0800 2020\n" +
            "\n" +
            "Log Length: 0\n" +
            "\n" +
            "\n" +
            "Log Type: stderr\n" +
            "\n" +
            "Log Upload Time: Mon May 18 16:11:22 +0800 2020\n" +
            "\n" +
            "Log Length: 3616\n" +
            "\n" +
            "20/05/18 16:11:18 INFO util.SignalUtils: Registered signal handler for TERM\n" +
            "20/05/18 16:11:18 INFO util.SignalUtils: Registered signal handler for HUP\n" +
            "20/05/18 16:11:18 INFO util.SignalUtils: Registered signal handler for INT";
    File yarnAppLogFile = new File(mTestDir, "yarn-application.log");
    writeToFile(yarnAppLogFile, yarnAppLog);
    LocalDateTime yarnAppDatetime = CollectLogCommand.inferFileStartTime(yarnAppLogFile);
    LocalDateTime expectedDatetime = LocalDateTime.of(2020, 5, 18, 16, 11, 18);
    assertThat(String.format("Expected datetime is %s but inferred %s from file%n", expectedDatetime, yarnAppDatetime),
            yarnAppDatetime, new DatetimeMatcher().setDatetime(expectedDatetime));

    // Yarn log default format
    String yarnLog = "2020-05-16 02:02:25,855 INFO org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl: container_e103_1584954066020_230169_01_000004 Container Transitioned from ALLOCATED to ACQUIRED\n" +
            "2020-05-16 02:02:25,909 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo: checking for deactivate... \n" +
            "2020-05-16 02:02:26,006 INFO org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl: container_e103_1584954066020_230168_01_000047 Container Transitioned from ALLOCATED to ACQUIRED";
    File yarnLogFile = new File(mTestDir, "yarn-rm.log");
    writeToFile(yarnLogFile, yarnLog);
    LocalDateTime yarnDatetime = CollectLogCommand.inferFileStartTime(yarnLogFile);
    expectedDatetime = LocalDateTime.of(2020, 5, 16, 2, 2, 25, 855 * 1_000_000);
    assertThat(String.format("Expected datetime is %s but inferred %s from file%n", expectedDatetime, yarnDatetime),
            yarnDatetime, new DatetimeMatcher().setDatetime(expectedDatetime));

    // ZK log default format
    String zkLog = "2020-05-14 21:05:53,822 WARN org.apache.zookeeper.server.NIOServerCnxn: caught end of stream exception\n" +
            "EndOfStreamException: Unable to read additional data from client sessionid 0x471fa7133c193e4, likely client has closed socket\n" +
            "\tat org.apache.zookeeper.server.NIOServerCnxn.doIO(NIOServerCnxn.java:241)\n" +
            "\tat org.apache.zookeeper.server.NIOServerCnxnFactory.run(NIOServerCnxnFactory.java:208)\n" +
            "\tat java.lang.Thread.run(Thread.java:748)\n" +
            "2020-05-14 21:05:53,823 INFO org.apache.zookeeper.server.NIOServerCnxn: Closed socket connection for client /10.64.23.190:50120 which had sessionid 0x471fa7133c193e4\n" +
            "2020-05-14 21:05:53,911 WARN org.apache.zookeeper.server.NIOServerCnxn: caught end of stream exception\n" +
            "EndOfStreamException: Unable to read additional data from client sessionid 0x471fa7133c193e1, likely client has closed socket\n" +
            "\tat org.apache.zookeeper.server.NIOServerCnxn.doIO(NIOServerCnxn.java:241)\n" +
            "\tat org.apache.zookeeper.server.NIOServerCnxnFactory.run(NIOServerCnxnFactory.java:208)\n" +
            "\tat java.lang.Thread.run(Thread.java:748)";
    File zkLogFile = new File(mTestDir, "zk.log");
    writeToFile(zkLogFile, zkLog);
    LocalDateTime zkDatetime = CollectLogCommand.inferFileStartTime(zkLogFile);
    expectedDatetime = LocalDateTime.of(2020, 5, 14, 21, 5, 53, 822 * 1_000_000);
    assertThat(String.format("Expected datetime is %s but inferred %s from file%n", expectedDatetime, zkDatetime),
            zkDatetime, new DatetimeMatcher().setDatetime(expectedDatetime));

    // HDFS log default format
    String hdfsLog = "2020-05-15 22:02:27,878 INFO BlockStateChange: BLOCK* addStoredBlock: blockMap updated: 10.64.23.184:1025 is added to blk_1126197354_52572663{blockUCState=UNDER_CONSTRUCTION, primaryNodeIndex=-1, replicas=[ReplicaUnderConstruction[[DISK]DS-46e3eb6e-7109-4a13-92dd-31b53658bfcf:NORMAL:10.64.23.124:1025|FINALIZED], ReplicaUnderConstruction[[DISK]DS-f2e3f3b1-73f7-4a75-b22d-e1067317469e:NORMAL:10.64.23.184:1025|FINALIZED]]} size 0\n" +
            "2020-05-15 22:02:27,878 INFO BlockStateChange: BLOCK* addStoredBlock: blockMap updated: 10.70.22.117:1025 is added to blk_1126197354_52572663{blockUCState=UNDER_CONSTRUCTION, primaryNodeIndex=-1, replicas=[ReplicaUnderConstruction[[DISK]DS-46e3eb6e-7109-4a13-92dd-31b53658bfcf:NORMAL:10.64.23.124:1025|FINALIZED], ReplicaUnderConstruction[[DISK]DS-f2e3f3b1-73f7-4a75-b22d-e1067317469e:NORMAL:10.64.23.184:1025|FINALIZED], ReplicaUnderConstruction[[DISK]DS-2ba9205e-24e7-4730-9c6c-6777d9e4bcdd:NORMAL:10.70.22.117:1025|FINALIZED]]} size 0";
    File hdfsLogFile = new File(mTestDir, "hdfs.log");
    writeToFile(hdfsLogFile, hdfsLog);
    LocalDateTime hdfsDatetime = CollectLogCommand.inferFileStartTime(hdfsLogFile);
    expectedDatetime = LocalDateTime.of(2020, 5, 15, 22, 2, 27, 878 * 1_000_000);
    assertThat(String.format("Expected datetime is %s but inferred %s from file%n", expectedDatetime, hdfsDatetime),
            hdfsDatetime, new DatetimeMatcher().setDatetime(expectedDatetime));

    // Presto log default format
    String prestoLog = "2020-05-16T00:00:01.059+0800\tINFO\tdispatcher-query-7960\tio.prestosql.event.QueryMonitor\tTIMELINE: Query 20200515_155959_06700_6r6b4 :: Transaction:[4d30e960-c319-439c-84dd-022ddab6fa5e] :: elapsed 1208ms :: planning 0ms :: waiting 0ms :: scheduling 1208ms :: running 0ms :: finishing 1208ms :: begin 2020-05-15T23:59:59.850+08:00 :: end 2020-05-16T00:00:01.058+08:00\n" +
            "2020-05-16T00:00:01.083+0800\tINFO\tdispatcher-query-7960\tcom.bluetalon.presto.authorization.BtStatementAccessControl\tStatementRewriteContext: FullStatementRewriteContext{queryId=20200515_160001_06701_6r6b4, principal=Optional[edsfuser@SVCS.DBS.COM], user=edsfuser, source=presto-jdbc, catalog=Optional[hive], schema=Optional[default], startTime=1589558401082, remoteUserAddress=10.70.23.171}\n" +
            "2020-05-16T00:00:01.083+0800\tINFO\tdispatcher-query-7960\tcom.bluetalon.presto.authorization.BtStatementAccessControl\tBtStatementAccessControl.getModifiedQuery() entering: user=edsfuser,principal=Optional[edsfuser@SVCS.DBS.COM],query=CREATE OR REPLACE VIEW P_S_SG.S_ADA_CASP_DATAPOST_MCASTMT_4_view_ns AS SELECT \"businessdate\",\"camstmfl_brch\",\"camstmfl_category_ident\",\"camstmfl_ccy\",\"camstmfl_ccy_desc\",\"camstmfl_chkdgt\",\"camstmfl_conv_currency\",\"camstmfl_conv_rate\",\"camstmfl_dept\",\"camstmfl_filehdr_sysid\",\"camstmfl_product_code\",\"camstmfl_rchq_reason\",\"camstmfl_rchq_reason_desc\",\"camstmfl_request_type\",\"camstmfl_reversal_ind\",\"camstmfl_sector_code\",\"camstmfl_serial\",\"camstmfl_serial_day\",\"camstmfl_serial_month\",\"camstmfl_serial_product\",\"camstmfl_serial_ref\",\"camstmfl_stmt_code\",\"camstmfl_stmt_date\",\"camstmfl_suffix\",\"camstmfl_summary_sysid\",\"camstmfl_suppress_ind\",\"camstmfl_total_records\",\"camstmfl_trans_amount\",\"camstmfl_trans_amount_dec\",\"camstmfl_trans_amount_sign\",\"camstmfl_trans_balance\",\"camstmfl_trans_balance_dec\",\"camstmfl_trans_balance_sign\",\"camstmfl_trans_date\",\"camstmfl_trans_dd\",\"camstmfl_trans_mm\",\"camstmfl_trans_type\",\"camstmfl_trans_yy\",\"camstmfl_value_date\",\"camstmfl_value_dd\",\"camstmfl_value_mm\",\"camstmfl_value_yy\",\"rowid\",\"rowmd5\",\"rowkey\",\"business_date\" FROM S_SG.S_ADA_CASP_DATAPOST_MCASTMT_4,catalog=hive,schema=default";
    File prestoLogFile = new File(mTestDir, "presto.log");
    writeToFile(prestoLogFile, prestoLog);
    LocalDateTime prestoDatetime = CollectLogCommand.inferFileStartTime(prestoLogFile);
    expectedDatetime = LocalDateTime.of(2020, 5, 16, 0, 0, 1, 59 * 1_000_000);
    assertThat(String.format("Expected datetime is %s but inferred %s from file%n", expectedDatetime, prestoDatetime),
            prestoDatetime, new DatetimeMatcher().setDatetime(expectedDatetime));

    // ParNew/CMS GC log default format
    String gcLog = "Java HotSpot(TM) 64-Bit Server VM (25.151-b12) for linux-amd64 JRE (1.8.0_151-b12), built on Sep  5 2017 19:20:58 by \"java_re\" with gcc 4.3.0 20080428 (Red Hat 4.3.0-8)\n" +
            "Memory: 4k page, physical 197920016k(194009624k free), swap 33279996k(33279996k free)\n" +
            "CommandLine flags: -XX:CMSInitiatingOccupancyFraction=65 -XX:InitialHeapSize=154618822656 -XX:MaxDirectMemorySize=103079215104 -XX:MaxHeapSize=154618822656 -XX:MaxNewSize=32212254720 -XX:MaxTenuringThreshold=6 -XX:MetaspaceSize=536870912 -XX:NewSize=32212254720 -XX:OldPLABSize=16 -XX:+PrintGC -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseCMSInitiatingOccupancyOnly -XX:+UseConcMarkSweepGC -XX:+UseParNewGC \n" +
            "2020-05-07T10:01:11.409+0800: 4.304: [GC (GCLocker Initiated GC) 2020-05-07T10:01:11.410+0800: 4.304: [ParNew: 25669137K->130531K(28311552K), 0.4330954 secs] 25669137K->130531K(147849216K), 0.4332463 secs] [Times: user=4.50 sys=0.08, real=0.44 secs] ";
    File gcLogFile = new File(mTestDir, "gc.log");
    writeToFile(gcLogFile, gcLog);
    LocalDateTime gcDatetime = CollectLogCommand.inferFileStartTime(gcLogFile);
    expectedDatetime = LocalDateTime.of(2020, 5, 7, 10, 1, 11, 409 * 1_000_000);
    assertThat(String.format("Expected datetime is %s but inferred %s from file%n", expectedDatetime, gcDatetime),
            gcDatetime, new DatetimeMatcher().setDatetime(expectedDatetime));
  }
}
