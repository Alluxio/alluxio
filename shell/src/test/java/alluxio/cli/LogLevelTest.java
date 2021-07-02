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

package alluxio.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.ClientContext;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.client.job.JobMasterClient;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.master.MasterInquireClient;
import alluxio.wire.WorkerNetAddress;

import org.apache.commons.cli.CommandLine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MasterInquireClient.Factory.class, JobMasterClient.Factory.class,
        FileSystemContext.class})
public class LogLevelTest {
  // Configure the web port to use special numbers to make sure the config is taking effect
  private static final int MASTER_WEB_PORT = 45699;
  private static final int WORKER_WEB_PORT = 50099;
  private static final int JOB_MASTER_WEB_PORT = 55699;
  private static final int JOB_WORKER_WEB_PORT = 60099;

  InstancedConfiguration mConf;

  @Before
  public void initConf() {
    mConf = InstancedConfiguration.defaults();
    mConf.set(PropertyKey.MASTER_WEB_PORT, MASTER_WEB_PORT);
    mConf.set(PropertyKey.WORKER_WEB_PORT, WORKER_WEB_PORT);
    mConf.set(PropertyKey.JOB_MASTER_WEB_PORT, JOB_MASTER_WEB_PORT);
    mConf.set(PropertyKey.JOB_WORKER_WEB_PORT, JOB_WORKER_WEB_PORT);
  }

  @Test
  public void parseSingleMasterTarget() throws Exception {
    mConf.set(PropertyKey.MASTER_HOSTNAME, "masters-1");

    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{"--target", "master"};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(LogLevel.TARGET_OPTION_NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(LogLevel.TARGET_OPTION_NAME)).thenReturn(mockArgs[1]);

    List<LogLevel.TargetInfo> targets = LogLevel.parseOptTarget(mockCommandLine, mConf);
    assertEquals(1, targets.size());
    assertEquals(new LogLevel.TargetInfo("masters-1", MASTER_WEB_PORT, "master"), targets.get(0));
  }

  @Test
  public void parseSingleJobMasterTarget() throws Exception {
    mConf.set(PropertyKey.MASTER_HOSTNAME, "masters-1");

    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{"--target", "job_master"};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(LogLevel.TARGET_OPTION_NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(LogLevel.TARGET_OPTION_NAME)).thenReturn(mockArgs[1]);

    List<LogLevel.TargetInfo> targets = LogLevel.parseOptTarget(mockCommandLine, mConf);
    assertEquals(1, targets.size());
    assertEquals(new LogLevel.TargetInfo("masters-1", JOB_MASTER_WEB_PORT, "job_master"),
            targets.get(0));
  }

  @Test
  public void parseZooKeeperHAMasterTarget() throws Exception {
    mConf.set(PropertyKey.ZOOKEEPER_ENABLED, true);
    mConf.set(PropertyKey.ZOOKEEPER_ADDRESS, "masters-1:2181");

    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{"--target", "master"};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(LogLevel.TARGET_OPTION_NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(LogLevel.TARGET_OPTION_NAME)).thenReturn(mockArgs[1]);

    PowerMockito.mockStatic(MasterInquireClient.Factory.class);
    MasterInquireClient mockInquireClient = mock(MasterInquireClient.class);
    when(mockInquireClient.getPrimaryRpcAddress()).thenReturn(
            new InetSocketAddress("masters-1", mConf.getInt(PropertyKey.MASTER_RPC_PORT)));
    when(MasterInquireClient.Factory.create(any(), any())).thenReturn(mockInquireClient);

    List<LogLevel.TargetInfo> targets = LogLevel.parseOptTarget(mockCommandLine, mConf);
    assertEquals(1, targets.size());
    assertEquals(new LogLevel.TargetInfo("masters-1", MASTER_WEB_PORT, "master"),
            targets.get(0));
  }

  @Test
  public void parseEmbeddedHAMasterTarget() throws Exception {
    mConf.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES, "masters-1:19200,masters-2:19200");

    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{"--target", "master"};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(LogLevel.TARGET_OPTION_NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(LogLevel.TARGET_OPTION_NAME)).thenReturn(mockArgs[1]);

    PowerMockito.mockStatic(MasterInquireClient.Factory.class);
    MasterInquireClient mockInquireClient = mock(MasterInquireClient.class);
    when(mockInquireClient.getPrimaryRpcAddress()).thenReturn(new InetSocketAddress("masters-1",
            mConf.getInt(PropertyKey.MASTER_RPC_PORT)));
    when(MasterInquireClient.Factory.create(any(), any())).thenReturn(mockInquireClient);

    List<LogLevel.TargetInfo> targets = LogLevel.parseOptTarget(mockCommandLine, mConf);
    assertEquals(1, targets.size());
    assertEquals(new LogLevel.TargetInfo("masters-1", MASTER_WEB_PORT, "master"),
            targets.get(0));
  }

  @Test
  public void parseZooKeeperHAJobMasterTarget() throws Exception {
    mConf.set(PropertyKey.ZOOKEEPER_ENABLED, true);
    mConf.set(PropertyKey.ZOOKEEPER_ADDRESS, "masters-1:2181");

    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{"--target", "job_master"};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(LogLevel.TARGET_OPTION_NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(LogLevel.TARGET_OPTION_NAME)).thenReturn(mockArgs[1]);

    PowerMockito.mockStatic(JobMasterClient.Factory.class);
    JobMasterClient mockJobClient = mock(JobMasterClient.class);
    when(mockJobClient.getAddress()).thenReturn(new InetSocketAddress("masters-2",
            mConf.getInt(PropertyKey.JOB_MASTER_RPC_PORT)));
    when(JobMasterClient.Factory.create(any())).thenReturn(mockJobClient);

    List<LogLevel.TargetInfo> targets = LogLevel.parseOptTarget(mockCommandLine, mConf);
    assertEquals(1, targets.size());
    assertEquals(new LogLevel.TargetInfo("masters-2", JOB_MASTER_WEB_PORT, "job_master"),
            targets.get(0));
  }

  @Test
  public void parseEmbeddedHAJobMasterTarget() throws Exception {
    mConf.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES, "masters-1:19200,masters-2:19200");

    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{"--target", "job_master"};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(LogLevel.TARGET_OPTION_NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(LogLevel.TARGET_OPTION_NAME)).thenReturn(mockArgs[1]);

    PowerMockito.mockStatic(JobMasterClient.Factory.class);
    JobMasterClient mockJobClient = mock(JobMasterClient.class);
    when(mockJobClient.getAddress()).thenReturn(new InetSocketAddress("masters-2",
            mConf.getInt(PropertyKey.JOB_MASTER_RPC_PORT)));
    when(JobMasterClient.Factory.create(any())).thenReturn(mockJobClient);

    List<LogLevel.TargetInfo> targets = LogLevel.parseOptTarget(mockCommandLine, mConf);
    assertEquals(1, targets.size());
    assertEquals(new LogLevel.TargetInfo("masters-2", JOB_MASTER_WEB_PORT, "job_master"),
            targets.get(0));
  }

  @Test
  public void parseWorkerTargets() throws Exception {
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{"--target", "workers"};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(LogLevel.TARGET_OPTION_NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(LogLevel.TARGET_OPTION_NAME)).thenReturn(mockArgs[1]);

    // Prepare a list of workers
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(new BlockWorkerInfo(new WorkerNetAddress()
            .setHost("workers-1").setWebPort(WORKER_WEB_PORT), 0, 0));
    workers.add(new BlockWorkerInfo(new WorkerNetAddress()
            .setHost("workers-2").setWebPort(WORKER_WEB_PORT), 0, 0));

    PowerMockito.mockStatic(FileSystemContext.class);
    FileSystemContext mockFsContext = mock(FileSystemContext.class);
    when(mockFsContext.getCachedWorkers()).thenReturn(workers);
    when(FileSystemContext.create(any(ClientContext.class))).thenReturn(mockFsContext);

    List<LogLevel.TargetInfo> targets = LogLevel.parseOptTarget(mockCommandLine, mConf);
    assertEquals(2, targets.size());
    assertEquals(new LogLevel.TargetInfo("workers-1", WORKER_WEB_PORT, "worker"),
            targets.get(0));
    assertEquals(new LogLevel.TargetInfo("workers-2", WORKER_WEB_PORT, "worker"),
            targets.get(1));
  }

  @Test
  public void parseJobWorkerTargets() throws Exception {
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{"--target", "job_workers"};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(LogLevel.TARGET_OPTION_NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(LogLevel.TARGET_OPTION_NAME)).thenReturn(mockArgs[1]);

    // Prepare a list of job workers
    List<JobWorkerHealth> jobWorkers = new ArrayList<>();
    jobWorkers.add(new JobWorkerHealth(0, new ArrayList<>(), 10, 0, 0, "workers-1"));
    jobWorkers.add(new JobWorkerHealth(1, new ArrayList<>(), 10, 0, 0, "workers-2"));

    PowerMockito.mockStatic(JobMasterClient.Factory.class);
    JobMasterClient mockJobClient = mock(JobMasterClient.class);
    when(mockJobClient.getAllWorkerHealth()).thenReturn(jobWorkers);
    when(JobMasterClient.Factory.create(any())).thenReturn(mockJobClient);

    List<LogLevel.TargetInfo> targets = LogLevel.parseOptTarget(mockCommandLine, mConf);
    assertEquals(2, targets.size());
    assertEquals(new LogLevel.TargetInfo("workers-1", JOB_WORKER_WEB_PORT, "job_worker"),
            targets.get(0));
    assertEquals(new LogLevel.TargetInfo("workers-2", JOB_WORKER_WEB_PORT, "job_worker"),
            targets.get(1));
  }

  @Test
  public void masterPlural() throws Exception {
    mConf.set(PropertyKey.MASTER_HOSTNAME, "masters-1");

    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{"--target", "masters,job_masters"};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(LogLevel.TARGET_OPTION_NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(LogLevel.TARGET_OPTION_NAME)).thenReturn(mockArgs[1]);

    List<LogLevel.TargetInfo> targets = LogLevel.parseOptTarget(mockCommandLine, mConf);
    assertEquals(2, targets.size());
    assertEquals(new HashSet<>(Arrays.asList(
            new LogLevel.TargetInfo("masters-1", MASTER_WEB_PORT, "master"),
            new LogLevel.TargetInfo("masters-1", JOB_MASTER_WEB_PORT, "job_master"))),
            new HashSet<>(targets));
  }

  @Test
  public void parsetManualTargets() throws Exception {
    // Successfully guess all targets
    // One extra comma at the end
    // Some extra whitespace
    String allTargets = "masters-1:" + MASTER_WEB_PORT + " ,masters-2:" + JOB_MASTER_WEB_PORT
            + " ,\tworkers-1:" + WORKER_WEB_PORT + ",workers-2:" + WORKER_WEB_PORT
            + ",workers-3:" + JOB_WORKER_WEB_PORT + ",workers-4:" + JOB_WORKER_WEB_PORT + ", ";

    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{"--target", allTargets};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(LogLevel.TARGET_OPTION_NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(LogLevel.TARGET_OPTION_NAME)).thenReturn(mockArgs[1]);

    List<LogLevel.TargetInfo> targets = LogLevel.parseOptTarget(mockCommandLine, mConf);
    assertEquals(6, targets.size());
    assertEquals(new HashSet<>(Arrays.asList(
            new LogLevel.TargetInfo("masters-1", MASTER_WEB_PORT, "master"),
            new LogLevel.TargetInfo("masters-2", JOB_MASTER_WEB_PORT, "job_master"),
            new LogLevel.TargetInfo("workers-1", WORKER_WEB_PORT, "worker"),
            new LogLevel.TargetInfo("workers-2", WORKER_WEB_PORT, "worker"),
            new LogLevel.TargetInfo("workers-3", JOB_WORKER_WEB_PORT, "job_worker"),
            new LogLevel.TargetInfo("workers-4", JOB_WORKER_WEB_PORT, "job_worker"))),
            new HashSet<>(targets));
  }

  @Test
  public void unrecognizedTarget() throws Exception {
    String allTargets = "localhost";
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{"--target", allTargets};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(LogLevel.TARGET_OPTION_NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(LogLevel.TARGET_OPTION_NAME)).thenReturn(mockArgs[1]);

    assertThrows("Unrecognized target argument: localhost", IOException.class, () ->
            LogLevel.parseOptTarget(mockCommandLine, mConf));
  }

  @Test
  public void unrecognizedPort() throws Exception {
    String allTargets = "localhost:12345";
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{"--target", allTargets};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption(LogLevel.TARGET_OPTION_NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(LogLevel.TARGET_OPTION_NAME)).thenReturn(mockArgs[1]);

    assertThrows("Unrecognized port in localhost:12345", IllegalArgumentException.class, () ->
            LogLevel.parseOptTarget(mockCommandLine, mConf));
  }
}
