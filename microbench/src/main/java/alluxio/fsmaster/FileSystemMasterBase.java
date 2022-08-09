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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetStatusPResponse;
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

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.rules.TemporaryFolder;

import java.net.URI;
import java.util.ArrayList;
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
  StreamObserver<GetStatusPResponse> mGetStatusObserver;

  /**
   * Initializes the journal system, the file system master and the file system master server.
   * Also launches the journal system. These are initialized in their own function as their
   * creation and launch is expensive and subject to failures.
   * @throws Exception if the journal system fails to be initialized and started
   */
  void init() throws Exception {
    Logger.getRootLogger().setLevel(Level.ERROR);
    mFolder.create();

    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
    Configuration.set(PropertyKey.WORK_DIR, mFolder.newFolder().getAbsolutePath());
    Configuration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
        mFolder.newFolder("FileSystemMasterTest").getAbsolutePath());
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
    mFsMaster = new DefaultFileSystemMaster("ClusterId", blockMaster, masterContext,
        ExecutorServiceFactories.constantExecutorServiceFactory(service));
    mFsMasterServer = new FileSystemMasterClientServiceHandler(mFsMaster);
    mGetStatusObserver = createStreamObserver();

    mRegistry.add(FileSystemMaster.class, mFsMaster);
    mJournalSystem.start();
    mJournalSystem.gainPrimacy();
    mRegistry.start(true);
  }

  private <T> ServerCallStreamObserver<T> createStreamObserver() {
    return new ServerCallStreamObserver<T>() {
      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public void setOnCancelHandler(Runnable onCancelHandler) {}

      @Override
      public void setCompression(String compression) {}

      @Override
      public boolean isReady() {
        return true;
      }

      @Override
      public void setOnReadyHandler(Runnable onReadyHandler) {}

      @Override
      public void disableAutoInboundFlowControl() {}

      @Override
      public void request(int count) {}

      @Override
      public void setMessageCompression(boolean enable) {}

      @Override
      public void onNext(T value) {}

      @Override
      public void onError(Throwable t) {
        throw new RuntimeException(t);
      }

      @Override
      public void onCompleted() {}
    };
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
  public void getStatus(int depth, long id) {
    String path = mDepthPaths.get(depth) + "file" + id;
    mFsMasterServer.getStatus(GetStatusPRequest.newBuilder().setPath(path).build(),
        mGetStatusObserver);
  }
}
