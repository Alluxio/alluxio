//package alluxio.server.ft.journal;
//
//import alluxio.AlluxioURI;
//import alluxio.Constants;
//import alluxio.UnderFileSystemFactoryRegistryRule;
//import alluxio.client.block.BlockMasterClient;
//import alluxio.client.file.FileSystem;
//import alluxio.conf.Configuration;
//import alluxio.conf.PropertyKey;
//import alluxio.master.EmbeddedJournalHALocalAlluxioCluster;
//import alluxio.master.LocalAlluxioMaster;
//import alluxio.server.ft.MasterFailoverIntegrationTest;
//import alluxio.testutils.BaseIntegrationTest;
//import alluxio.testutils.IntegrationTestUtils;
//import alluxio.testutils.underfs.delegating.DelegatingUnderFileSystem;
//import alluxio.testutils.underfs.delegating.DelegatingUnderFileSystemFactory;
//import alluxio.underfs.UnderFileSystem;
//import alluxio.underfs.options.DeleteOptions;
//import alluxio.util.CommonUtils;
//import alluxio.worker.WorkerProcess;
//import com.google.common.io.Files;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.ClassRule;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.TestName;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//
//public class RatisFailureRecoveryIntegrationTest extends BaseIntegrationTest {
//
//  private static final int NUM_MASTERS = 3;
//  private static final int NUM_WORKERS = 1;
//
//  ExecutorService mExecutor = Executors.newFixedThreadPool(1);
//
//  private static final Logger LOG = LoggerFactory.getLogger(MasterFailoverIntegrationTest.class);
//
//  private static final String LOCAL_UFS_PATH = Files.createTempDir().getAbsolutePath();
//  private static final long DELETE_DELAY = 5 * Constants.SECOND_MS;
//
//  private EmbeddedJournalHALocalAlluxioCluster mCluster;
//  private FileSystem mFileSystem;
//
//  private List<Thread> mWorkerThreads;
//  private List<WorkerProcess> mWorkers;
//
//  // An under file system which has slow directory deletion.
//  private static final UnderFileSystem UFS =
//          new DelegatingUnderFileSystem(UnderFileSystem.Factory.create(LOCAL_UFS_PATH,
//                  Configuration.global())) {
//            @Override
//            public boolean deleteDirectory(String path) throws IOException {
//              CommonUtils.sleepMs(DELETE_DELAY);
//              return mUfs.deleteDirectory(path);
//            }
//
//            @Override
//            public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
//              CommonUtils.sleepMs(DELETE_DELAY);
//              return mUfs.deleteDirectory(path, options);
//            }
//          };
//
//  @Rule
//  public TestName mTestName = new TestName();
//
//  @ClassRule
//  public static UnderFileSystemFactoryRegistryRule sUnderfilesystemfactoryregistry =
//          new UnderFileSystemFactoryRegistryRule(new DelegatingUnderFileSystemFactory(UFS));
//
//  @Before
//  public final void before() throws Exception {
//    // We do not start workers in the cluster, but manually create them here to test
//    mCluster = new EmbeddedJournalHALocalAlluxioCluster(3);
//    mCluster.initConfiguration(
//            IntegrationTestUtils.getTestName(getClass().getSimpleName(), mTestName.getMethodName()));
//    Configuration.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "60sec");
//    Configuration.set(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "1sec");
//    Configuration.set(PropertyKey.NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT, "30sec");
//    Configuration.set(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, "0sec");
//    Configuration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
//            DelegatingUnderFileSystemFactory.DELEGATING_SCHEME + "://" + LOCAL_UFS_PATH);
//    mCluster.start();
//    mFileSystem = mCluster.getClient();
//
//    mWorkerThreads = new ArrayList<>();
//    mWorkers = new ArrayList<>();
//  }
//
//  @After
//  public final void after() throws Exception {
//    mCluster.stop();
//    mExecutor.shutdown();
//  }
//
//  @Test
//  public void ratisCrash() throws Exception {
//    mFileSystem.exists(new AlluxioURI("/"));
//
//    List<LocalAlluxioMaster> masters = mCluster.getMasters();
//    for (int i = 0; i < 3; i++) {
//      LocalAlluxioMaster m = masters.get(i);
//      System.out.format("Master %d journal at %s%n", i, m.getJournalFolder());
//    }
//  }
//}
