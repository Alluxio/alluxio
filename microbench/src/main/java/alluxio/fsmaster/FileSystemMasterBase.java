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

package alluxio.fsmaster;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetStatusPResponse;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.FileSystemMasterClientServiceHandler;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalType;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.user.TestUserState;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableMap;
import io.grpc.stub.StreamObserver;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.rules.TemporaryFolder;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileSystemMasterBase {
  // field initialized in constructor
  private final TemporaryFolder mFolder = new TemporaryFolder();
  private final MasterRegistry mRegistry = new MasterRegistry();
  private final ArrayList<String> mDepthPaths = new ArrayList<>();

  // fields initialized in #init()
  private JournalSystem mJournalSystem;
  FileSystemMaster mFsMaster;
  FileSystemMasterClientServiceHandler mFsMasterServer;

  /**
   * Initializes the journal system, the file system master and the file system master server.
   * Also launches the journal system. These are initialized in their own function as their
   * creation and launch is expensive and subject to failures.
   * @throws Exception if the journal system fails to be initialized and started
   */
  void init() throws Exception {
    Logger.getRootLogger().setLevel(Level.ERROR);
    mFolder.create();

    ConfigurationRule config = new ConfigurationRule(new HashMap<PropertyKey, Object>() {
      {
        put(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
        put(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "000");
        put(PropertyKey.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS, 20);
        put(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, 0);
        put(PropertyKey.WORK_DIR, mFolder.newFolder().getAbsolutePath());
        put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
            mFolder.newFolder("FileSystemMasterTest").getAbsolutePath());
        put(PropertyKey.MASTER_FILE_SYSTEM_OPERATION_RETRY_CACHE_ENABLED, false);
      }
    }, Configuration.modifiableGlobal());
    config.before();
    AuthenticatedClientUser.set("test");

    mJournalSystem = new JournalSystem.Builder()
        .setLocation(new URI(mFolder.newFolder().getAbsolutePath()))
        .setQuietTimeMs(0)
        .build(CommonUtils.ProcessType.MASTER);
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext(mJournalSystem,
        new TestUserState("test", Configuration.global()));
    MetricsMaster metricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mRegistry.add(MetricsMaster.class, metricsMaster);
    BlockMaster blockMaster = new BlockMasterFactory().create(mRegistry, masterContext);

    ExecutorService service = Executors.newFixedThreadPool(4,
        ThreadFactoryUtils.build("DefaultFileSystemMasterTest-%d", true));
    mFsMaster = new DefaultFileSystemMaster(blockMaster, masterContext,
        ExecutorServiceFactories.constantExecutorServiceFactory(service));
    mFsMasterServer = new FileSystemMasterClientServiceHandler(mFsMaster);

    mRegistry.add(FileSystemMaster.class, mFsMaster);
    mJournalSystem.start();
    mJournalSystem.gainPrimacy();
    mRegistry.start(true);

    // set up workers
    long workerId1 = blockMaster.getWorkerId(
        new WorkerNetAddress().setHost("localhost").setRpcPort(80).setDataPort(81).setWebPort(82));
    blockMaster.workerRegister(workerId1,
        Arrays.asList(Constants.MEDIUM_MEM, Constants.MEDIUM_SSD),
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.MB,
            Constants.MEDIUM_SSD, (long) Constants.MB),
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB,
            Constants.MEDIUM_SSD, (long) Constants.KB),
        ImmutableMap.of(), new HashMap<>(), RegisterWorkerPOptions.getDefaultInstance());
    long workerId2 = blockMaster.getWorkerId(
        new WorkerNetAddress().setHost("remote").setRpcPort(80).setDataPort(81).setWebPort(82));
    blockMaster.workerRegister(workerId2,
        Arrays.asList(Constants.MEDIUM_MEM, Constants.MEDIUM_SSD),
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.MB,
            Constants.MEDIUM_SSD, (long) Constants.MB),
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB,
            Constants.MEDIUM_SSD, (long) Constants.KB),
        ImmutableMap.of(), new HashMap<>(), RegisterWorkerPOptions.getDefaultInstance());
  }

  public void tearDown() throws Exception {
    mRegistry.stop();
    mJournalSystem.stop();
    mFsMaster.close();
    mFsMaster.stop();
    mFolder.delete();
    Configuration.reloadProperties();
  }

  // used for setup
  public void createPathDepths(int depth) throws Exception {
    mDepthPaths.ensureCapacity(depth + 1);
    StringBuilder pathBuilder = new StringBuilder("/");
    mDepthPaths.add(pathBuilder.toString());
    for (int i = 0; i < depth; i++) {
      pathBuilder.append("next/");
      String pathDepthI = pathBuilder.toString();
      mDepthPaths.add(pathDepthI);
      mFsMaster.createDirectory(new AlluxioURI(pathDepthI), CreateDirectoryContext.defaults());
    }
  }

  // used for setup
  public void createFile(int depth, long id) throws Exception {
    AlluxioURI uri = new AlluxioURI(mDepthPaths.get(depth) + "file" + id);
    mFsMaster.createFile(uri, CreateFileContext.defaults());
  }

  // used for benchmark
  public void getStatus(int depth, long id, StreamObserver<GetStatusPResponse> responseObserver) {
    String path = mDepthPaths.get(depth) + "file" + id;
    mFsMasterServer.getStatus(GetStatusPRequest.newBuilder().setPath(path).build(),
        responseObserver);
  }
}
