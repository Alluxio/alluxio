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
package tachyon.master;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

import tachyon.Constants;
import tachyon.UnderFileSystemCluster;
import tachyon.UnderFileSystemsUtils;
import tachyon.client.TachyonFS;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;

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

  private LocalTachyonMaster(final String tachyonHome, TachyonConf tachyonConf) throws IOException {
    mTachyonHome = tachyonHome;

    mDataDir = path(mTachyonHome, "data");
    mLogDir = path(mTachyonHome, "logs");

    UnderFileSystemsUtils.mkdirIfNotExists(mDataDir, tachyonConf);
    UnderFileSystemsUtils.mkdirIfNotExists(mLogDir, tachyonConf);

    mHostname = NetworkUtils.getLocalHostName();

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

    UnderFileSystemsUtils.mkdirIfNotExists(mJournalFolder, tachyonConf);
    CommonUtils.touch(mJournalFolder + "/_format_" + System.currentTimeMillis(), tachyonConf);

    tachyonConf.set(Constants.MASTER_HOSTNAME, mHostname);
    tachyonConf.set(Constants.MASTER_JOURNAL_FOLDER, mJournalFolder);
    tachyonConf.set(Constants.UNDERFS_ADDRESS, mUnderFSFolder);

    tachyonConf.set(Constants.MASTER_PORT, "0");
    tachyonConf.set(Constants.MASTER_WEB_PORT, "0");

    tachyonConf.set(Constants.MASTER_MIN_WORKER_THREADS, "1");
    tachyonConf.set(Constants.MASTER_MAX_WORKER_THREADS, "100");

    // If tests fail to connect they should fail early rather than using the default ridiculously high retries
    tachyonConf.set(Constants.MASTER_RETRY_COUNT, "3");

    tachyonConf.set(Constants.MASTER_WEB_THREAD_COUNT, "1");
    tachyonConf.set(Constants.WEB_RESOURCES, System.getProperty("user.dir") + "/src/main/webapp");

    mTachyonMaster = new TachyonMaster(tachyonConf);

    // Reset the ports
    tachyonConf.set(Constants.MASTER_PORT, Integer.toString(getMetaPort()));
    tachyonConf.set(Constants.MASTER_WEB_PORT, Integer.toString(getMetaPort() + 1));

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
   * @throws IOException unable to do file operation or listen on port
   */
  public static LocalTachyonMaster create(TachyonConf tachyonConf) throws IOException {
    final String tachyonHome = uniquePath();
    UnderFileSystemsUtils.deleteDir(tachyonHome, tachyonConf);
    UnderFileSystemsUtils.mkdirIfNotExists(tachyonHome, tachyonConf);

    // Update Tachyon home in the passed TachyonConf instance.
    tachyonConf.set(Constants.TACHYON_HOME, tachyonHome);

    return new LocalTachyonMaster(tachyonHome, tachyonConf);
  }

  /**
   * Creates a new local tachyon master with a isolated port. tachyonHome is expected to be clean
   * before calling this method.
   * <p />
   * Clean is defined as
   * 
   * <pre>
   * {@code
   *   UnderFileSystems.deleteDir(tachyonHome);
   *   UnderFileSystems.mkdirIfNotExists(tachyonHome);
   * }
   * </pre>
   * 
   * @throws IOException unable to do file operation or listen on port
   */
  public static LocalTachyonMaster create(final String tachyonHome, TachyonConf tachyonConf)
      throws IOException {
    return new LocalTachyonMaster(Preconditions.checkNotNull(tachyonHome), tachyonConf);
  }

  public void start() {
    mMasterThread.start();
  }

  /**
   * Stops the master and cleans up client connections.
   * 
   * This method will not clean up {@link tachyon.UnderFileSystemsUtils} data. To do that you must
   * call {@link #cleanupUnderfs()}.
   */
  public void stop() throws Exception {
    clearClients();

    mTachyonMaster.stop();

    System.clearProperty("tachyon.web.resources");
    System.clearProperty("tachyon.master.min.worker.threads");

  }

  public void clearClients() throws IOException {
    mClientPool.close();
  }

  public void cleanupUnderfs() throws IOException {
    if (null != mUnderFSCluster) {
      mUnderFSCluster.cleanup();
    }
    System.clearProperty("tachyon.underfs.address");
  }

  public int getMetaPort() {
    return mTachyonMaster.getMetaPort();
  }

  public String getUri() {
    return Constants.HEADER + mHostname + ":" + getMetaPort();
  }

  public TachyonFS getClient() throws IOException {
    return mClientPool.getClient(mTachyonMaster.getTachyonConf());
  }

  public String getEditLogPath() {
    return mUnderFSCluster.getUnderFilesystemAddress() + "/journal/log.data";
  }

  public String getImagePath() {
    return mUnderFSCluster.getUnderFilesystemAddress() + "/journal/image.data";
  }

  public MasterInfo getMasterInfo() {
    return mTachyonMaster.getMasterInfo();
  }

  private static String uniquePath() throws IOException {
    return File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.nanoTime();
  }

  private static String path(final String parent, final String child) {
    return parent + "/" + child;
  }

  public boolean isStarted() {
    return mTachyonMaster.isStarted();
  }

  public TachyonConf getTachyonConf() {
    return mTachyonMaster.getTachyonConf();
  }
}
