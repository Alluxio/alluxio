package alluxio.fsmaster;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.journal.JournalSystem;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.security.user.TestUserState;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileSystemMasterBase {
  private final String mJournalFolder = Paths.get("/tmp", "fsmasterbench").toString();
  private final MasterRegistry mRegistry = new MasterRegistry();
  private final ExecutorService mExecutorService = Executors
      .newFixedThreadPool(4, ThreadFactoryUtils.build("DefaultFileSystemMasterTest-%d", true));

  private final JournalSystem mJournalSystem;
  private final MetricsMaster mMetricsMaster;
  private final BlockMaster mBlockMaster;
  private final long mWorkerId1;
  private final long mWorkerId2;

  FileSystemMaster mFsMaster;

  FileSystemMasterBase() throws Exception {
    new File(mJournalFolder).mkdirs();
    mJournalSystem = new JournalSystem.Builder()
        .setLocation(new URI(mJournalFolder))
        .setQuietTimeMs(0)
        .build(CommonUtils.ProcessType.MASTER);
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext(mJournalSystem,
        new TestUserState("test", ServerConfiguration.global()));
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mRegistry.add(MetricsMaster.class, mMetricsMaster);
    mBlockMaster = new BlockMasterFactory().create(mRegistry, masterContext);

    mFsMaster = new DefaultFileSystemMaster(mBlockMaster, masterContext,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mRegistry.add(FileSystemMaster.class, mFsMaster);
    mJournalSystem.start();
    mJournalSystem.gainPrimacy();
    mRegistry.start(true);

    // set up workers
    mWorkerId1 = mBlockMaster.getWorkerId(
        new WorkerNetAddress().setHost("localhost").setRpcPort(80).setDataPort(81).setWebPort(82));
    mBlockMaster.workerRegister(mWorkerId1,
        Arrays.asList(Constants.MEDIUM_MEM, Constants.MEDIUM_SSD),
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.MB,
            Constants.MEDIUM_SSD, (long) Constants.MB),
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB,
            Constants.MEDIUM_SSD, (long) Constants.KB),
        ImmutableMap.of(), new HashMap<>(), RegisterWorkerPOptions.getDefaultInstance());
    mWorkerId2 = mBlockMaster.getWorkerId(
        new WorkerNetAddress().setHost("remote").setRpcPort(80).setDataPort(81).setWebPort(82));
    mBlockMaster.workerRegister(mWorkerId2,
        Arrays.asList(Constants.MEDIUM_MEM, Constants.MEDIUM_SSD),
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.MB,
            Constants.MEDIUM_SSD, (long) Constants.MB),
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB,
            Constants.MEDIUM_SSD, (long) Constants.KB),
        ImmutableMap.of(), new HashMap<>(), RegisterWorkerPOptions.getDefaultInstance());
  }

  public FileInfo createFile(int id) throws Exception {
    return mFsMaster.createFile(new AlluxioURI("/file" + id), CreateFileContext.defaults());
  }

  public FileInfo getFileInfo(int id) throws Exception {
    return mFsMaster.getFileInfo(new AlluxioURI("/file" + id), GetStatusContext.defaults());
  }
}
