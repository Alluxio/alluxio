/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

import tachyon.Constants;
import tachyon.UnderFileSystemCluster;
import tachyon.UnderFileSystemsUtils;
import tachyon.client.TachyonFS;
import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.conf.UserConf;
import tachyon.conf.WorkerConf;
import tachyon.util.CommonUtils;

/**
 * Constructs an isolated master. Primary users of this class are the
 * {@link tachyon.master.LocalTachyonCluster} and
 * {@link tachyon.master.LocalTachyonClusterMultiMaster}.
 * 
 * Isolated is defined as having its own root directory, and port.
 */
public final class LocalTachyonMaster {
  // TODO should this be moved to TachyonURI? Prob after UFS supports it

  private final String TACHYON_HOME;
  private final String DATA_DIR;
  private final String LOG_DIR;
  private final String HOSTNAME;

  private final UnderFileSystemCluster UNDERFS_CLUSTER;
  private final String UNDERFS_FOLDER;
  private final String JOURNAL_FOLDER;

  private final TachyonMaster MASTER;
  private final Thread MASTER_THREAD;

  private final Supplier<String> CLIENT_SUPPLIER = new Supplier<String>() {
    @Override
    public String get() {
      return getUri();
    }
  };
  private final ClientPool CLIENT_POOL = new ClientPool(CLIENT_SUPPLIER);

  private LocalTachyonMaster(final String tachyonHome) throws IOException {
    TACHYON_HOME = tachyonHome;

    DATA_DIR = path(TACHYON_HOME, "data");
    LOG_DIR = path(TACHYON_HOME, "logs");

    UnderFileSystemsUtils.mkdirIfNotExists(DATA_DIR);
    UnderFileSystemsUtils.mkdirIfNotExists(LOG_DIR);

    HOSTNAME = InetAddress.getLocalHost().getCanonicalHostName();

    // To start the UFS either for integration or unit test. If it targets the unit test, UFS is
    // setup over the local file system (see also {@link LocalFilesystemCluster} - under folder of
    // "mTachyonHome/tachyon*". Otherwise, it starts some distributed file system cluster e.g.,
    // miniDFSCluster (see also {@link tachyon.LocalMiniDFScluster} and setup the folder like
    // "hdfs://xxx:xxx/tachyon*".
    UNDERFS_CLUSTER = UnderFileSystemCluster.get(TACHYON_HOME + "/dfs");
    UNDERFS_FOLDER = UNDERFS_CLUSTER.getUnderFilesystemAddress() + "/tachyon_underfs_folder";
    // To setup the journalFolder under either local file system or distributed ufs like
    // miniDFSCluster
    JOURNAL_FOLDER = UNDERFS_CLUSTER.getUnderFilesystemAddress() + "/journal";

    UnderFileSystemsUtils.mkdirIfNotExists(JOURNAL_FOLDER);
    CommonUtils.touch(JOURNAL_FOLDER + "/_format_" + System.currentTimeMillis());

    System.setProperty("tachyon.master.hostname", HOSTNAME);
    System.setProperty("tachyon.master.journal.folder", JOURNAL_FOLDER);
    System.setProperty("tachyon.underfs.address", UNDERFS_FOLDER);

    CommonConf.clear();
    MasterConf.clear();
    WorkerConf.clear();
    UserConf.clear();

    MASTER = new TachyonMaster(new InetSocketAddress(HOSTNAME, 0), 0, 1, 1, 1);

    System.setProperty("tachyon.master.port", Integer.toString(getMetaPort()));
    System.setProperty("tachyon.master.web.port", Integer.toString(getMetaPort() + 1));

    Runnable runMaster = new Runnable() {
      @Override
      public void run() {
        try {
          MASTER.start();
        } catch (Exception e) {
          throw new RuntimeException(e + " \n Start Master Error \n" + e.getMessage(), e);
        }
      }
    };

    MASTER_THREAD = new Thread(runMaster);
  }

  /**
   * Creates a new local tachyon master with a isolated home and port.
   * 
   * @throws IOException
   *           unable to do file operation or listen on port
   */
  public static LocalTachyonMaster create() throws IOException {
    final String tachyonHome = uniquePath();
    UnderFileSystemsUtils.deleteDir(tachyonHome);
    UnderFileSystemsUtils.mkdirIfNotExists(tachyonHome);

    System.setProperty("tachyon.home", tachyonHome);

    return new LocalTachyonMaster(tachyonHome);
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
   * @throws IOException
   *           unable to do file operation or listen on port
   */
  public static LocalTachyonMaster create(final String tachyonHome) throws IOException {
    return new LocalTachyonMaster(Preconditions.checkNotNull(tachyonHome));
  }

  public void start() {
    MASTER_THREAD.start();
  }

  /**
   * Stops the master and cleans up client connections.
   * 
   * This method will not clean up {@link tachyon.UnderFileSystemsUtils} data. To do that you must
   * call {@link #cleanupUnderfs()}.
   */
  public void stop() throws Exception {
    clearClients();

    MASTER.stop();

    System.clearProperty("tachyon.home");
    System.clearProperty("tachyon.master.hostname");
    System.clearProperty("tachyon.master.port");
  }

  public void clearClients() throws IOException {
    CLIENT_POOL.close();
  }

  public void cleanupUnderfs() throws IOException {
    if (null != UNDERFS_CLUSTER) {
      UNDERFS_CLUSTER.cleanup();
    }
    System.clearProperty("tachyon.master.journal.folder");
    System.clearProperty("tachyon.underfs.address");
  }

  public int getMetaPort() {
    return MASTER.getMetaPort();
  }

  public String getUri() {
    return Constants.HEADER + HOSTNAME + ":" + getMetaPort();
  }

  public TachyonFS getClient() throws IOException {
    return CLIENT_POOL.getClient();
  }

  public String getEditLogPath() {
    return UNDERFS_CLUSTER.getUnderFilesystemAddress() + "/journal/log.data";
  }

  public String getImagePath() {
    return UNDERFS_CLUSTER.getUnderFilesystemAddress() + "/journal/image.data";
  }

  public MasterInfo getMasterInfo() {
    return MASTER.getMasterInfo();
  }

  private static String uniquePath() throws IOException {
    return File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.nanoTime();
  }

  private static String path(final String parent, final String child) {
    return parent + "/" + child;
  }

  public boolean isStarted() {
    return MASTER.isStarted();
  }
}
