package alluxio.fsmaster;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
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
import alluxio.master.journal.JournalType;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.user.TestUserState;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableMap;
import org.junit.rules.TemporaryFolder;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileSystemMasterBase {
  private final TemporaryFolder mFolder = new TemporaryFolder();
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
    }, ServerConfiguration.global());
    config.before();
    AuthenticatedClientUser.set("test");

    mJournalSystem = new JournalSystem.Builder()
        .setLocation(new URI(mFolder.newFolder().getAbsolutePath()))
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

  public void tearDown() throws Exception {
    mRegistry.stop();
    mJournalSystem.stop();
    mFsMaster.close();
    mFsMaster.stop();
    mFolder.delete();
    ServerConfiguration.reset();
  }

  public FileInfo createFile(long id) throws Exception {
    return mFsMaster.createFile(new AlluxioURI("/file" + id), CreateFileContext.defaults());
  }

  public FileInfo getFileInfo(long id) throws Exception {
    return mFsMaster.getFileInfo(new AlluxioURI("/file" + id), GetStatusContext.defaults());
  }
}
