package tachyon.master;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import tachyon.Constants;
import tachyon.UnderFileSystemCluster;
import tachyon.UnderFileSystems;
import tachyon.client.TachyonFS;
import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.conf.UserConf;
import tachyon.conf.WorkerConf;
import tachyon.util.CommonUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Constructs an isolated master.  Primary users of this class are the
 * {@link tachyon.master.LocalTachyonCluster} and
 * {@link tachyon.master.LocalTachyonClusterMultiMaster}.
 *
 * Isolated is defined as having its own root directory, and port.
 */
public final class LocalTachyonMaster {
  //TODO should this be moved to TachyonURI?  Prob after UFS supports it

  private final String mTachyonHome;
  private final String mData;
  private final String mLogs;
  private final String mLocalhostName;

  private final UnderFileSystemCluster mUnderFSCluster;
  private final String mUnderfsFolder;
  private final String mJournalFolder;

  private final TachyonMaster mMaster;
  private final Thread mMasterThread;

  private final Supplier<String> clientSupplier = new Supplier<String>() {
    @Override
    public String get() {
      return getUri();
    }
  };
  private final ClientPool clientPool = new ClientPool(clientSupplier);

  private LocalTachyonMaster(final String tachyonHome) throws IOException {
    mTachyonHome = tachyonHome;

    mData = path(mTachyonHome, "data");
    mLogs = path(mTachyonHome, "logs");

    UnderFileSystems.mkdirIfNotExists(mData);
    UnderFileSystems.mkdirIfNotExists(mLogs);

    mLocalhostName = InetAddress.getLocalHost().getCanonicalHostName();

    // To start the UFS either for integration or unit test. If it targets the unit test, UFS is
    // setup over the local file system (see also {@link LocalFilesystemCluster} - under folder of
    // "mTachyonHome/tachyon*". Otherwise, it starts some distributed file system cluster e.g.,
    // miniDFSCluster (see also {@link tachyon.LocalMiniDFScluster} and setup the folder like
    // "hdfs://xxx:xxx/tachyon*".
    mUnderFSCluster = UnderFileSystemCluster.get(mTachyonHome + "/dfs");
    mUnderfsFolder = mUnderFSCluster.getUnderFilesystemAddress() + "/tachyon_underfs_folder";
    // To setup the journalFolder under either local file system or distributed ufs like
    // miniDFSCluster
    mJournalFolder = mUnderFSCluster.getUnderFilesystemAddress() + "/journal";

    UnderFileSystems.mkdirIfNotExists(mJournalFolder);
    CommonUtils.touch(mJournalFolder + "/_format_" + System.currentTimeMillis());

    System.setProperty("tachyon.master.hostname", mLocalhostName);
    System.setProperty("tachyon.master.journal.folder", mJournalFolder);
    System.setProperty("tachyon.underfs.address", mUnderfsFolder);

    CommonConf.clear();
    MasterConf.clear();
    WorkerConf.clear();
    UserConf.clear();

    mMaster =
        new TachyonMaster(new InetSocketAddress(mLocalhostName, 0), 0, 1,
            1, 1);

    System.setProperty("tachyon.master.port", Integer.toString(getPort()));
    System.setProperty("tachyon.master.web.port", Integer.toString(getPort() + 1));

    Runnable runMaster = new Runnable() {
      @Override
      public void run() {
        try {
          mMaster.start();
        } catch (Exception e) {
          throw new RuntimeException(e + " \n Start Master Error \n" + e.getMessage(), e);
        }
      }
    };

    mMasterThread = new Thread(runMaster);
  }

  /**
   * Creates a new local tachyon master with a isolated home and port.
   *
   * @throws IOException unable to do file operation or listen on port
   */
  public static LocalTachyonMaster create() throws IOException {
    final String tachyonHome = uniquePath();
    UnderFileSystems.deleteDir(tachyonHome);
    UnderFileSystems.mkdirIfNotExists(tachyonHome);

    System.setProperty("tachyon.home", tachyonHome);

    return new LocalTachyonMaster(tachyonHome);
  }

  /**
   * Creates a new local tachyon master with a isolated port.  tachyonHome is expected to be clean
   * before calling this method.
   * <p />
   * Clean is defined as
   * <pre>{@code
   *   UnderFileSystems.deleteDir(tachyonHome);
   *   UnderFileSystems.mkdirIfNotExists(tachyonHome);
   * }</pre>
   *
   * @throws IOException unable to do file operation or listen on port
   */
  public static LocalTachyonMaster create(final String tachyonHome) throws IOException {
    return new LocalTachyonMaster(Preconditions.checkNotNull(tachyonHome));
  }

  public void start() {
    mMasterThread.start();
  }

  /**
   * Stops the master and cleans up client connections.
   *
   * This method will not clean up {@link tachyon.UnderFileSystems} data.  To do that you must call
   * {@link #cleanupUnderfs()}.
   */
  public void stop() throws Exception {
    clearClients();

    mMaster.stop();

    System.clearProperty("tachyon.home");
    System.clearProperty("tachyon.master.hostname");
    System.clearProperty("tachyon.master.port");
  }

  public void clearClients() throws IOException {
    clientPool.close();
  }

  public void cleanupUnderfs() throws IOException {
    if (null != mUnderFSCluster) {
      mUnderFSCluster.cleanup();
    }
    System.clearProperty("tachyon.master.journal.folder");
    System.clearProperty("tachyon.underfs.address");
  }

  public int getPort() {
    return mMaster.getLocalPort();
  }

  public String getUri() {
    return Constants.HEADER + mLocalhostName + ":" + getPort();
  }

  public TachyonFS getClient() throws IOException {
    return clientPool.getClient();
  }

  public String getEditLogPath() {
    return mUnderFSCluster.getUnderFilesystemAddress() + "/journal/log.data";
  }

  public String getImagePath() {
    return mUnderFSCluster.getUnderFilesystemAddress() + "/journal/image.data";
  }

  public MasterInfo getMasterInfo() {
    return mMaster.getMasterInfo();
  }

  private static String uniquePath() throws IOException {
    return File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.nanoTime();
  }

  private static String path(final String parent, final String child) {
    return parent + "/" + child;
  }

  public boolean isStarted() {
    return mMaster.isStarted();
  }
}
