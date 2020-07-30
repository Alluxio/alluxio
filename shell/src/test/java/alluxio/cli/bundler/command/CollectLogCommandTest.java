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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.cli.bundler.InfoCollectorTestUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;

import org.apache.commons.cli.CommandLine;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CollectLogCommandTest {
  private static final int MILLISEC_TO_NANOSEC = 1_000_000;

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
    for (String s : CollectLogCommand.FILE_NAMES_PREFIXES) {
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
    Set<String> createdFiles = InfoCollectorTestUtils
            .getAllFileNamesRelative(testLogDir, testLogDir);
    mExpectedFiles = createdFiles;

    return testLogDir;
  }

  @Test
  public void logFilesCopied() throws IOException, AlluxioException {
    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));

    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{cmd.getCommandName(), targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(targetDir, cmd.getCommandName());

    InfoCollectorTestUtils.verifyAllFiles(subDir, mExpectedFiles);
  }

  @Test
  public void irrelevantFileIgnored() throws Exception {
    // This file will not be copied
    // Not included in the expected set
    InfoCollectorTestUtils.createFileInDir(mTestDir, "irrelevant");

    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{cmd.getCommandName(), targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(targetDir, cmd.getCommandName());

    InfoCollectorTestUtils.verifyAllFiles(subDir, mExpectedFiles);
  }

  @Test
  public void fileNameExcluded() throws Exception {
    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{
            cmd.getCommandName(),
            targetDir.getAbsolutePath()
    };
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(eq("exclude-logs"))).thenReturn(true);
    when(mockCommandLine.getOptionValue(eq("exclude-logs"))).thenReturn("master.log.1, worker");
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(targetDir, cmd.getCommandName());
    mExpectedFiles.remove("master.log.1");
    mExpectedFiles.remove("worker.log");
    mExpectedFiles.remove("worker.out");
    mExpectedFiles.remove("worker.log.backup");

    InfoCollectorTestUtils.verifyAllFiles(subDir, mExpectedFiles);
  }

  @Test
  public void fileNameAdded() throws Exception {
    InfoCollectorTestUtils.createFileInDir(mTestDir, "alluxio_gc.log");
    InfoCollectorTestUtils.createFileInDir(mTestDir, "alluxio_gc.log.1");
    InfoCollectorTestUtils.createFileInDir(mTestDir, "alluxio_gc.log.2");

    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{
            cmd.getCommandName(),
            targetDir.getAbsolutePath()
    };
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(eq("additional-logs"))).thenReturn(true);
    when(mockCommandLine.getOptionValue(eq("additional-logs"))).thenReturn("alluxio_gc");
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(targetDir, cmd.getCommandName());
    mExpectedFiles.add("alluxio_gc.log");
    mExpectedFiles.add("alluxio_gc.log.1");
    mExpectedFiles.add("alluxio_gc.log.2");

    InfoCollectorTestUtils.verifyAllFiles(subDir, mExpectedFiles);
  }

  @Test
  public void fileNameReplaced() throws Exception {
    InfoCollectorTestUtils.createFileInDir(mTestDir, "alluxio_gc.log");
    InfoCollectorTestUtils.createFileInDir(mTestDir, "alluxio_gc.log.1");
    InfoCollectorTestUtils.createFileInDir(mTestDir, "alluxio_gc.log.2");

    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{
            cmd.getCommandName(),
            targetDir.getAbsolutePath()
    };
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(eq("include-logs"))).thenReturn(true);
    when(mockCommandLine.getOptionValue(eq("include-logs"))).thenReturn("alluxio_gc, master");
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(targetDir, cmd.getCommandName());
    mExpectedFiles = new HashSet<>();
    mExpectedFiles.add("alluxio_gc.log");
    mExpectedFiles.add("alluxio_gc.log.1");
    mExpectedFiles.add("alluxio_gc.log.2");
    mExpectedFiles.add("master.log");
    mExpectedFiles.add("master.log.1");
    mExpectedFiles.add("master.log.2");
    mExpectedFiles.add("master.out");
    mExpectedFiles.add("master_audit.log");

    InfoCollectorTestUtils.verifyAllFiles(subDir, mExpectedFiles);
  }

  @Test
  public void illegalSelectorCombinations() throws Exception {
    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    String[] mockArgs = new String[]{
            cmd.getCommandName(),
            targetDir.getAbsolutePath()
    };

    // --include-logs will replace the wanted list
    // It is ambiguous to use it with --exclude-logs
    CommandLine includeExclude = mock(CommandLine.class);
    when(includeExclude.getArgs()).thenReturn(mockArgs);
    when(includeExclude.hasOption(eq("include-logs"))).thenReturn(true);
    when(includeExclude.getOptionValue(eq("include-logs"))).thenReturn("alluxio_gc, master");
    when(includeExclude.hasOption(eq("exclude-logs"))).thenReturn(true);
    when(includeExclude.getOptionValue(eq("exclude-logs"))).thenReturn("master");
    assertNotEquals(0, cmd.run(includeExclude));

    // --include-logs will replace the wanted list
    // It is ambiguous to use it with --additional-logs
    CommandLine includeAddition = mock(CommandLine.class);
    when(includeAddition.getArgs()).thenReturn(mockArgs);
    when(includeAddition.hasOption(eq("include-logs"))).thenReturn(true);
    when(includeAddition.getOptionValue(eq("include-logs"))).thenReturn("alluxio_gc, master");
    when(includeAddition.hasOption(eq("additional-logs"))).thenReturn(true);
    when(includeAddition.getOptionValue(eq("additional-logs"))).thenReturn("worker");
    assertNotEquals(0, cmd.run(includeExclude));
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
    for (File f : CommonUtils.recursiveListLocalDir(mTestDir)) {
      f.setLastModified(logEndTimestamp);
    }

    // Some logs are created after this time, should be ignored
    File masterLog = new File(mTestDir, "master.log");
    String log = "2020-07-08 18:53:45,129 INFO  CopycatServer - Server started successfully!\n"
            + "2020-07-08 18:53:59,129 INFO  RaftJournalSystem - Started Raft Journal System..\n"
            + "2020-07-09 00:01:59,135 INFO  DefaultMetaMaster - Standby master with address...\n"
            + "2020-07-09 00:03:59,135 INFO  AlluxioMasterProcess - All masters started\n"
            + "2020-07-09 01:12:59,138 INFO  AbstractPrimarySelector - Primary selector..\n"
            + "2020-07-09 01:53:59,139 INFO  AbstractMaster - TableMaster: Stopped secondary..";
    writeToFile(masterLog, log);
    masterLog.setLastModified(LocalDateTime.of(2020, 7, 9, 1, 53, 59)
            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

    // Some logs overlap with the end time
    File masterLog1 = new File(mTestDir, "master.log.1");
    String log1 = "2020-07-08 16:53:44,639 INFO  BlockMasterFactory - Creating..\n"
            + "2020-07-08 16:53:44,639 INFO  TableMasterFactory - Creating..\n"
            + "2020-07-08 17:01:44,639 INFO  MetaMasterFactory - Creating..\n"
            + "2020-07-08 17:01:45,639 INFO  FileSystemMasterFactory - Creating..\n"
            + "2020-07-08 18:53:44,751 INFO  UnderDatabaseRegistry - Registered UDBs: hive,glue\n"
            + "2020-07-08 18:53:44,756 INFO  LayoutRegistry - Registered Table Layouts: hive";
    writeToFile(masterLog1, log1);
    masterLog1.setLastModified(LocalDateTime.of(2020, 7, 8, 18, 53, 45)
            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

    // Some logs end before this end time
    File masterLog2 = new File(mTestDir, "master.log.2");
    String log2 = "2020-07-08 15:40:45,656 INFO  HdfsUnderFileSystem - Successfully..\n"
            + "2020-07-08 15:51:45,782 INFO  RocksStore - Opened rocks database under path..\n"
            + "2020-07-08 15:53:45,862 INFO  RocksStore - Opened rocks database under path..\n"
            + "2020-07-08 16:52:41,876 INFO  RocksStore - Opened rocks database under path..\n"
            + "2020-07-08 16:53:43,876 INFO  JournalStateMachine - Initialized new journal..";
    writeToFile(masterLog2, log2);
    masterLog1.setLastModified(LocalDateTime.of(2020, 7, 8, 16, 53, 44)
            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{
            cmd.getCommandName(),
            targetDir.getAbsolutePath()
    };
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(eq("end-time"))).thenReturn(true);
    when(mockCommandLine.getOptionValue(eq("end-time"))).thenReturn(issueEnd.format(fmt));
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(targetDir, cmd.getCommandName());
    mExpectedFiles.remove("master.log");
    InfoCollectorTestUtils.verifyAllFiles(subDir, mExpectedFiles);
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
    for (File f : CommonUtils.recursiveListLocalDir(mTestDir)) {
      f.setLastModified(logEndTimestamp);
    }

    // Some logs are created after this time, should be included
    File masterLog = new File(mTestDir, "master.log");
    String log = "2020-07-08 18:53:45,129 INFO  CopycatServer - Server started successfully!\n"
            + "2020-07-08 18:53:59,129 INFO  RaftJournalSystem - Started Raft Journal System..\n"
            + "2020-07-09 00:01:59,135 INFO  DefaultMetaMaster - Standby master with address..\n"
            + "2020-07-09 00:03:59,135 INFO  AlluxioMasterProcess - All masters started\n"
            + "2020-07-09 01:12:59,138 INFO  AbstractPrimarySelector - Primary selector..\n"
            + "2020-07-09 01:53:59,139 INFO  AbstractMaster - TableMaster: Stopped..";
    writeToFile(masterLog, log);
    masterLog.setLastModified(LocalDateTime.of(2020, 7, 9, 1, 53, 59)
            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

    // Some logs overlap with the start time, should be included
    File masterLog1 = new File(mTestDir, "master.log.1");
    String log1 = "2020-07-08 16:53:44,639 INFO  BlockMasterFactory - Creating..\n"
            + "2020-07-08 16:53:44,639 INFO  TableMasterFactory - Creating..\n"
            + "2020-07-08 17:01:44,639 INFO  MetaMasterFactory - Creating..\n"
            + "2020-07-08 17:01:45,639 INFO  FileSystemMasterFactory - Creating..\n"
            + "2020-07-08 18:53:44,751 INFO  UnderDatabaseRegistry - Registered UDBs: hive,glue\n"
            + "2020-07-08 18:53:44,756 INFO  LayoutRegistry - Registered Table Layouts: hive";
    writeToFile(masterLog1, log1);
    masterLog1.setLastModified(LocalDateTime.of(2020, 7, 8, 18, 53, 45)
            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

    // Some logs end before the start time, should be ignored
    File masterLog2 = new File(mTestDir, "master.log.2");
    String log2 = "2020-07-08 15:40:45,656 INFO  HdfsUnderFileSystem - Successfully..\n"
            + "2020-07-08 15:51:45,782 INFO  RocksStore - Opened rocks database under path..\n"
            + "2020-07-08 15:53:45,862 INFO  RocksStore - Opened rocks database under path..\n"
            + "2020-07-08 16:52:41,876 INFO  RocksStore - Opened rocks database under path..\n"
            + "2020-07-08 16:53:43,876 INFO  JournalStateMachine - Initialized new..";
    writeToFile(masterLog2, log2);
    masterLog2.setLastModified(LocalDateTime.of(2020, 7, 8, 16, 53, 44)
            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

    // Copy
    CollectLogCommand cmd = new CollectLogCommand(FileSystemContext.create(mConf));
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{
            cmd.getCommandName(),
            targetDir.getAbsolutePath()
    };
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(eq("start-time"))).thenReturn(true);
    when(mockCommandLine.getOptionValue(eq("start-time"))).thenReturn(issueEnd.format(fmt));
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(targetDir, cmd.getCommandName());
    mExpectedFiles = new HashSet<>();
    mExpectedFiles.add("master.log");
    mExpectedFiles.add("master.log.1");
    InfoCollectorTestUtils.verifyAllFiles(subDir, mExpectedFiles);
  }

  /**
   * A LocalDateTime matcher for assertThat.
   * */
  class DatetimeMatcher extends TypeSafeMatcher<LocalDateTime> {
    private LocalDateTime mDatetime;

    DatetimeMatcher setDatetime(LocalDateTime datetime) {
      mDatetime = datetime;
      return this;
    }

    @Override
    protected boolean matchesSafely(LocalDateTime s) {
      return mDatetime.isEqual(s);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("only digits");
    }
  }

  @Test
  public void inferDateFromLog() throws Exception {
    // A piece of Alluxio log with default format
    String alluxioLog = "2020-03-19 11:58:10,104 WARN  ServerConfiguration - Reloaded properties\n"
            + "2020-03-19 11:58:10,106 WARN  ServerConfiguration - Loaded hostname localhost\n"
            + "2020-03-19 11:58:10,591 WARN  RetryUtils - Failed to load cluster default..";
    File alluxioLogFile = new File(mTestDir, "alluxio-worker.log");
    writeToFile(alluxioLogFile, alluxioLog);
    LocalDateTime alluxioLogDatetime = CollectLogCommand.inferFileStartTime(alluxioLogFile);
    LocalDateTime expectedDatetime = LocalDateTime.of(2020, 3, 19, 11, 58, 10,
            104 * MILLISEC_TO_NANOSEC);
    assertThat(String.format("Expected datetime is %s but inferred %s from file%n",
            expectedDatetime, alluxioLogDatetime),
            alluxioLogDatetime,
            new DatetimeMatcher().setDatetime(expectedDatetime));

    // A piece of sample Yarn application log with default format
    // The first >20 lines are un-timestamped information about the job
    String yarnAppLog = "\nLogged in as: user\nApplication\nAbout\nJobs\nTools\n"
            + "Log Type: container-localizer-syslog\n\n"
            + "Log Upload Time: Mon May 18 16:11:22 +0800 2020\n\n"
            + "Log Length: 0\n\n\nLog Type: stderr\n\n"
            + "Log Upload Time: Mon May 18 16:11:22 +0800 2020\n\n"
            + "Log Length: 3616\n\n"
            + "20/05/18 16:11:18 INFO util.SignalUtils: Registered signal handler for TERM\n"
            + "20/05/18 16:11:18 INFO util.SignalUtils: Registered signal handler for HUP\n"
            + "20/05/18 16:11:18 INFO util.SignalUtils: Registered signal handler for INT";
    File yarnAppLogFile = new File(mTestDir, "yarn-application.log");
    writeToFile(yarnAppLogFile, yarnAppLog);
    LocalDateTime yarnAppDatetime = CollectLogCommand.inferFileStartTime(yarnAppLogFile);
    expectedDatetime = LocalDateTime.of(2020, 5, 18, 16, 11, 18);
    assertThat(String.format("Expected datetime is %s but inferred %s from file%n",
            expectedDatetime, yarnAppDatetime),
            yarnAppDatetime,
            new DatetimeMatcher().setDatetime(expectedDatetime));

    // A piece of Yarn log with default format
    String yarnLog = "2020-05-16 02:02:25,855 INFO org.apache.hadoop.yarn.server.resourcemanager"
            + ".rmcontainer.RMContainerImpl: container_e103_1584954066020_230169_01_000004 "
            + "Container Transitioned from ALLOCATED to ACQUIRED\n"
            + "2020-05-16 02:02:25,909 INFO org.apache.hadoop.yarn.server.resourcemanager"
            + ".scheduler.AppSchedulingInfo: checking for deactivate... \n"
            + "2020-05-16 02:02:26,006 INFO org.apache.hadoop.yarn.server.resourcemanager"
            + ".rmcontainer.RMContainerImpl: container_e103_1584954066020_230168_01_000047 "
            + "Container Transitioned from ALLOCATED to ACQUIRED";
    File yarnLogFile = new File(mTestDir, "yarn-rm.log");
    writeToFile(yarnLogFile, yarnLog);
    LocalDateTime yarnDatetime = CollectLogCommand.inferFileStartTime(yarnLogFile);
    expectedDatetime = LocalDateTime.of(2020, 5, 16, 2, 2, 25,
            855 * MILLISEC_TO_NANOSEC);
    assertThat(String.format("Expected datetime is %s but inferred %s from file%n",
            expectedDatetime, yarnDatetime),
            yarnDatetime,
            new DatetimeMatcher().setDatetime(expectedDatetime));

    // A piece of ZK log with default format
    String zkLog = "2020-05-14 21:05:53,822 WARN org.apache.zookeeper.server.NIOServerCnxn: "
            + "caught end of stream exception\n"
            + "EndOfStreamException: Unable to read additional data from client sessionid.., "
            + "likely client has closed socket\n"
            + "\tat org.apache.zookeeper.server.NIOServerCnxn.doIO(NIOServerCnxn.java:241)\n"
            + "\tat org.apache.zookeeper.server.NIOServerCnxnFactory.run(NIOServerCnxnFactory..)\n"
            + "\tat java.lang.Thread.run(Thread.java:748)\n"
            + "2020-05-14 21:05:53,823 INFO org.apache.zookeeper.server.NIOServerCnxn: "
            + "Closed socket connection for client /10.64.23.190:50120 which had sessionid..\n"
            + "2020-05-14 21:05:53,911 WARN org.apache.zookeeper.server.NIOServerCnxn: "
            + "caught end of stream exception\n"
            + "EndOfStreamException: Unable to read additional data from client sessionid.."
            + "likely client has closed socket\n";
    File zkLogFile = new File(mTestDir, "zk.log");
    writeToFile(zkLogFile, zkLog);
    LocalDateTime zkDatetime = CollectLogCommand.inferFileStartTime(zkLogFile);
    expectedDatetime = LocalDateTime.of(2020, 5, 14, 21, 5, 53,
            822 * MILLISEC_TO_NANOSEC);
    assertThat(String.format("Expected datetime is %s but inferred %s from file%n",
            expectedDatetime, zkDatetime),
            zkDatetime,
            new DatetimeMatcher().setDatetime(expectedDatetime));

    // A piece of sample HDFS log with default format
    String hdfsLog = "2020-05-15 22:02:27,878 INFO BlockStateChange: BLOCK* addStoredBlock: "
            + "blockMap updated: 10.64.23.184:1025 is added to blk_1126197354_52572663 size 0..\n"
            + "2020-05-15 22:02:27,878 INFO BlockStateChange: BLOCK* addStoredBlock: blockMap "
            + "updated: 10.70.22.117:1025 is added to blk_1126197354_52572663 size 0..";
    File hdfsLogFile = new File(mTestDir, "hdfs.log");
    writeToFile(hdfsLogFile, hdfsLog);
    LocalDateTime hdfsDatetime = CollectLogCommand.inferFileStartTime(hdfsLogFile);
    expectedDatetime = LocalDateTime.of(2020, 5, 15, 22, 2, 27,
            878 * MILLISEC_TO_NANOSEC);
    assertThat(String.format("Expected datetime is %s but inferred %s from file%n",
            expectedDatetime, hdfsDatetime),
            hdfsDatetime,
            new DatetimeMatcher().setDatetime(expectedDatetime));

    // A piece of sample Presto log wtih default format
    String prestoLog = "2020-05-16T00:00:01.059+0800\tINFO\tdispatcher-query-7960"
            + "\tio.prestosql.event.QueryMonitor\tTIMELINE: Query 20200515_155959_06700_6r6b4"
            + " :: Transaction:[4d30e960-c319-439c-84dd-022ddab6fa5e] :: elapsed 1208ms :: "
            + "planning 0ms :: waiting 0ms :: scheduling 1208ms :: running 0ms :: finishing 1208ms"
            + " :: begin 2020-05-15T23:59:59.850+08:00 :: end 2020-05-16T00:00:01.058+08:00\n"
            + "2020-05-16T00:00:03.530+0800\tINFO\tdispatcher-query-7948\t"
            + "io.prestosql.event.QueryMonitor\tTIMELINE: Query 20200515_160001_06701_6r6b4"
            + " :: Transaction:[be9b396e-6697-4ecd-9782-2ca1174c1be1] :: elapsed 2316ms"
            + " :: planning 0ms :: waiting 0ms :: scheduling 2316ms :: running 0ms"
            + " :: finishing 2316ms :: begin 2020-05-16T00:00:01.212+08:00"
            + " :: end 2020-05-16T00:00:03.528+08:00";
    File prestoLogFile = new File(mTestDir, "presto.log");
    writeToFile(prestoLogFile, prestoLog);
    LocalDateTime prestoDatetime = CollectLogCommand.inferFileStartTime(prestoLogFile);
    expectedDatetime = LocalDateTime.of(2020, 5, 16, 0, 0, 1,
            59 * MILLISEC_TO_NANOSEC);
    assertThat(String.format("Expected datetime is %s but inferred %s from file%n",
            expectedDatetime, prestoDatetime),
            prestoDatetime,
            new DatetimeMatcher().setDatetime(expectedDatetime));

    // ParNew/CMS GC log default format
    String gcLog = "Java HotSpot(TM) 64-Bit Server VM (25.151-b12) for linux-amd64 JRE (1.8.0), "
            + "built on Sep  5 2017 19:20:58 by \"java_re\" with gcc 4.3.0 20080428..\n"
            + "Memory: 4k page, physical 1979200k(1940096k free), swap 332799k(332799k free)\n"
            + "CommandLine flags: -XX:CMSInitiatingOccupancyFraction=65 -XX:InitialHeapSize=..\n"
            + "2020-05-07T10:01:11.409+0800: 4.304: [GC (GCLocker Initiated GC) "
            + "2020-05-07T10:01:11.410+0800: 4.304: [ParNew: 25669137K->130531K(28311552K), "
            + "0.4330954 secs] 25669137K->130531K(147849216K), 0.4332463 secs] "
            + "[Times: user=4.50 sys=0.08, real=0.44 secs] ";
    File gcLogFile = new File(mTestDir, "gc.log");
    writeToFile(gcLogFile, gcLog);
    LocalDateTime gcDatetime = CollectLogCommand.inferFileStartTime(gcLogFile);
    expectedDatetime = LocalDateTime.of(2020, 5, 7, 10, 1, 11,
            409 * MILLISEC_TO_NANOSEC);
    assertThat(String.format("Expected datetime is %s but inferred %s from file%n",
            expectedDatetime, gcDatetime),
            gcDatetime,
            new DatetimeMatcher().setDatetime(expectedDatetime));
  }

  @Test
  public void commandLineDatetime() throws Exception {
    Map<String, LocalDateTime> stringToDatetime = new HashMap<>();
    stringToDatetime.put("2020-06-27 11:58:53,084",
            LocalDateTime.of(2020, 6, 27, 11, 58, 53, 84 * MILLISEC_TO_NANOSEC));
    stringToDatetime.put("2020-06-27 11:58:53",
            LocalDateTime.of(2020, 6, 27, 11, 58, 53));
    stringToDatetime.put("2020-06-27 11:58",
            LocalDateTime.of(2020, 6, 27, 11, 58));
    stringToDatetime.put("20/06/27 11:58:53",
            LocalDateTime.of(2020, 6, 27, 11, 58, 53));
    stringToDatetime.put("20/06/27 11:58",
            LocalDateTime.of(2020, 6, 27, 11, 58));
    stringToDatetime.put("2020-06-27T11:58:53.084+0800",
            LocalDateTime.of(2020, 6, 27, 11, 58, 53, 84 * MILLISEC_TO_NANOSEC));
    stringToDatetime.put("2020-06-27T11:58:53",
            LocalDateTime.of(2020, 6, 27, 11, 58, 53));
    stringToDatetime.put("2020-06-27T11:58",
            LocalDateTime.of(2020, 6, 27, 11, 58));

    for (Map.Entry<String, LocalDateTime> entry : stringToDatetime.entrySet()) {
      String key = entry.getKey();
      LocalDateTime expectedDatetime = entry.getValue();
      LocalDateTime parsedDatetime = CollectLogCommand.parseDateTime(key);
      assertThat(String.format("Expected datetime is %s but parsed %s from string %s%n",
              expectedDatetime, parsedDatetime, key),
              parsedDatetime,
              new DatetimeMatcher().setDatetime(expectedDatetime));
    }
  }

  private void writeToFile(File f, String content) throws IOException {
    try (FileWriter writer = new FileWriter(f)) {
      writer.write(content);
    }
  }
}
