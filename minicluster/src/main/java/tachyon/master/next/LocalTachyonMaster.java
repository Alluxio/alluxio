/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.next;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

import tachyon.Constants;
import tachyon.client.TachyonFS;
import tachyon.client.next.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystemCluster;
import tachyon.util.io.PathUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import tachyon.util.UnderFileSystemUtils;

/**
 * Constructs an isolated master. Primary users of this class are the
 * {@link tachyon.master.LocalTachyonCluster} and
 * {@link tachyon.master.LocalTachyonClusterMultiMaster}.
 *
 * Isolated is defined as having its own root directory, and port.
 */
public final class LocalTachyonMaster {
  // TODO should this be moved to TachyonURI? Prob after UFS supports it

  private final String mTachyonHome;
  private final String mDataDir;
  private final String mLogDir;
  private final String mHostname;

  private final UnderFileSystemCluster mUnderFSCluster;
  private final String mUnderFSFolder;
  private final String mJournalFolder;

  private final TachyonMaster mTachyonMaster;
  private final Thread mMasterThread;

  private final Supplier<String> mClientSupplier = new Supplier<String>() {
    @Override
    public String get() {
      return getUri();
    }
  };
  private final ClientPool mClientPool = new ClientPool(mClientSupplier);

  private final tachyon.master.ClientPool mOldClientPool = new tachyon.master.ClientPool(
      mClientSupplier);

  private LocalTachyonMaster(final String tachyonHome, TachyonConf tachyonConf)
      throws IOException {
    mTachyonHome = tachyonHome;

    mDataDir = path(mTachyonHome, "data");
    mLogDir = path(mTachyonHome, "logs");

    UnderFileSystemUtils.mkdirIfNotExists(mDataDir, tachyonConf);
    UnderFileSystemUtils.mkdirIfNotExists(mLogDir, tachyonConf);

    mHostname = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC, tachyonConf);

    // To start the UFS either for integration or unit test. If it targets the unit test, UFS is
    // setup over the local file system (see also {@link LocalFilesystemCluster} - under folder of
    // "mTachyonHome/tachyon*". Otherwise, it starts some distributed file system cluster e.g.,
    // miniDFSCluster (see also {@link tachyon.LocalMiniDFScluster} and setup the folder like
    // "hdfs://xxx:xxx/tachyon*".
    mUnderFSCluster = UnderFileSystemCluster.get(mTachyonHome + "/dfs", tachyonConf);
    mUnderFSFolder = mUnderFSCluster.getUnderFilesystemAddress() + "/tachyon_underfs_folder";
    // To setup the journalFolder under either local file system or distributed ufs like
    // miniDFSCluster
    mJournalFolder = mUnderFSCluster.getUnderFilesystemAddress() + "/journal";

    UnderFileSystemUtils.mkdirIfNotExists(mJournalFolder, tachyonConf);
    String[] masterServiceNames = new String[] {
        Constants.BLOCK_MASTER_SERVICE_NAME,
        Constants.FILE_SYSTEM_MASTER_SERVICE_NAME,
        Constants.RAW_TABLE_MASTER_SERVICE_NAME,
        Constants.USER_MASTER_SERVICE_NAME,
    };
    for (String masterServieName : masterServiceNames) {
      UnderFileSystemUtils.mkdirIfNotExists(PathUtils.concatPath(mJournalFolder, masterServieName),
          tachyonConf);
    }
    UnderFileSystemUtils.touch(mJournalFolder + "/_format_" + System.currentTimeMillis(),
        tachyonConf);

    tachyonConf.set(Constants.MASTER_JOURNAL_FOLDER, mJournalFolder);
    tachyonConf.set(Constants.UNDERFS_ADDRESS, mUnderFSFolder);

    tachyonConf.set(Constants.MASTER_MIN_WORKER_THREADS, "1");
    tachyonConf.set(Constants.MASTER_MAX_WORKER_THREADS, "100");

    // If tests fail to connect they should fail early rather than using the default ridiculously
    // high retries
    tachyonConf.set(Constants.MASTER_RETRY_COUNT, "3");

    // Since tests are always running on a single host keep the resolution timeout low as otherwise
    // people running with strange network configurations will see very slow tests
    tachyonConf.set(Constants.HOST_RESOLUTION_TIMEOUT_MS, "250");

    tachyonConf.set(Constants.WEB_THREAD_COUNT, "1");
    tachyonConf.set(Constants.WEB_RESOURCES,
        PathUtils.concatPath(System.getProperty("user.dir"), "../servers/src/main/webapp"));

    mTachyonMaster = new TachyonMaster(tachyonConf);

    // Reset the master port
    tachyonConf.set(Constants.MASTER_PORT, Integer.toString(getRPCLocalPort()));

    Runnable runMaster = new Runnable() {
      @Override
      public void run() {
        try {
          mTachyonMaster.start();
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
   * @param tachyonConf Tachyon configuration
   * @throws IOException when unable to do file operation or listen on port
   * @return an instance of Tachyon master
   */
  public static LocalTachyonMaster create(TachyonConf tachyonConf) throws IOException {
    final String tachyonHome = uniquePath();
    UnderFileSystemUtils.deleteDir(tachyonHome, tachyonConf);
    UnderFileSystemUtils.mkdirIfNotExists(tachyonHome, tachyonConf);

    // Update Tachyon home in the passed TachyonConf instance.
    tachyonConf.set(Constants.TACHYON_HOME, tachyonHome);

    return new LocalTachyonMaster(tachyonHome, tachyonConf);
  }

  /**
   * Creates a new local tachyon master with a isolated port.
   *
   * @param tachyonHome Tachyon home directory
   * @param tachyonConf Tachyon configuration
   * @return an instance of Tachyon master
   * @throws IOException when unable to do file operation or listen on port
   */
  public static LocalTachyonMaster create(final String tachyonHome, TachyonConf tachyonConf)
      throws IOException {
    UnderFileSystemUtils.deleteDir(tachyonHome, tachyonConf);
    UnderFileSystemUtils.mkdirIfNotExists(tachyonHome, tachyonConf);

    return new LocalTachyonMaster(Preconditions.checkNotNull(tachyonHome), tachyonConf);
  }

  public void start() {
    mMasterThread.start();
  }

  public boolean isServing() {
    return mTachyonMaster.isServing();
  }

  /**
   * Stops the master and cleans up client connections.
   *
   * This method will not clean up {@link tachyon.util.UnderFileSystemUtils} data. To do that you
   * must call {@link #cleanupUnderfs()}.
   *
   * @throws Exception when the operation fails
   */
  public void stop() throws Exception {
    clearClients();

    mTachyonMaster.stop();

    System.clearProperty("tachyon.web.resources");
    System.clearProperty("tachyon.master.min.worker.threads");

  }

  public void clearClients() throws IOException {
    mClientPool.close();
    mOldClientPool.close();
  }

  public void cleanupUnderfs() throws IOException {
    if (null != mUnderFSCluster) {
      mUnderFSCluster.cleanup();
    }
    System.clearProperty("tachyon.underfs.address");
  }

  /**
   * Get the externally resolvable address of the master (used by unit test only).
   */
  public InetSocketAddress getAddress() {
    return mTachyonMaster.getMasterAddress();
  }

  /**
   * Gets the actual internal master.
   */
  public TachyonMaster getInternalMaster() {
    return mTachyonMaster;
  }

  /**
   * Get the actual bind hostname on RPC service (used by unit test only).
   *
   * @return the RPC bind hostname
   */
  public String getRPCBindHost() {
    return mTachyonMaster.getRPCBindHost();
  }

  /**
   * Get the actual port that the RPC service is listening on (used by unit test only)
   *
   * @return the RPC local port
   */
  public int getRPCLocalPort() {
    return mTachyonMaster.getRPCLocalPort();
  }

  /**
   * Get the actual bind hostname on web service (used by unit test only).
   *
   * @return the Web bind hostname
   */
  public String getWebBindHost() {
    return mTachyonMaster.getWebBindHost();
  }

  /**
   * Get the actual port that the web service is listening on (used by unit test only)
   *
   * @return the Web local port
   */
  public int getWebLocalPort() {
    return mTachyonMaster.getWebLocalPort();
  }

  public String getUri() {
    return Constants.HEADER + mHostname + ":" + getRPCLocalPort();
  }

  public TachyonFS getOldClient() throws IOException {
    return mOldClientPool.getClient(mTachyonMaster.getTachyonConf());
  }

  public TachyonFileSystem getClient() throws IOException {
    return mClientPool.getClient(mTachyonMaster.getTachyonConf());
  }

  private static String uniquePath() throws IOException {
    return File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.nanoTime();
  }

  private static String path(final String parent, final String child) {
    return parent + "/" + child;
  }

  public TachyonConf getTachyonConf() {
    return mTachyonMaster.getTachyonConf();
  }

  public String getJournalFolder() {
    return mJournalFolder;
  }
}
