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
import java.net.InetSocketAddress;

import com.google.common.base.Supplier;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.file.FileSystem;
import tachyon.conf.TachyonConf;
import tachyon.util.UnderFileSystemUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Constructs an isolated master. Primary users of this class are the {@link LocalTachyonCluster}
 * and {@link LocalTachyonClusterMultiMaster}.
 *
 * Isolated is defined as having its own root directory, and port.
 */
@NotThreadSafe
public final class LocalTachyonMaster {
  private final String mHostname;

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

  private LocalTachyonMaster()
      throws IOException {
    TachyonConf tachyonConf = MasterContext.getConf();
    mHostname = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC, tachyonConf);

    mJournalFolder = tachyonConf.get(Constants.MASTER_JOURNAL_FOLDER);

    mTachyonMaster = TachyonMaster.Factory.create();

    // Reset the master port
    tachyonConf.set(Constants.MASTER_RPC_PORT, Integer.toString(getRPCLocalPort()));

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
   * @throws IOException when unable to do file operation or listen on port
   * @return an instance of Tachyon master
   */
  public static LocalTachyonMaster create() throws IOException {
    final String tachyonHome = uniquePath();
    TachyonConf tachyonConf = MasterContext.getConf();
    UnderFileSystemUtils.deleteDir(tachyonHome, tachyonConf);
    UnderFileSystemUtils.mkdirIfNotExists(tachyonHome, tachyonConf);

    // Update Tachyon home in the passed TachyonConf instance.
    tachyonConf.set(Constants.TACHYON_HOME, tachyonHome);

    return new LocalTachyonMaster();
  }

  /**
   * Creates a new local tachyon master with a isolated port.
   *
   * @param tachyonHome Tachyon home directory, if the directory already exists, this method will
   *                    reuse any directory/file if possible, no deletion will be made
   * @return an instance of Tachyon master
   * @throws IOException when unable to do file operation or listen on port
   */
  public static LocalTachyonMaster create(final String tachyonHome) throws IOException {
    TachyonConf tachyonConf = MasterContext.getConf();
    UnderFileSystemUtils.mkdirIfNotExists(tachyonHome, tachyonConf);

    return new LocalTachyonMaster();
  }

  /**
   * Starts the master.
   */
  public void start() {
    mMasterThread.start();
  }

  /**
   * @return true if the master is serving, false otherwise
   */
  public boolean isServing() {
    return mTachyonMaster.isServing();
  }

  /**
   * Stops the master and cleans up client connections.
   *
   * @throws Exception when the operation fails
   */
  public void stop() throws Exception {
    clearClients();

    mTachyonMaster.stop();

    System.clearProperty("tachyon.web.resources");
    System.clearProperty("tachyon.master.min.worker.threads");

  }

  /**
   * Kill the master thread, by calling {@link Thread#interrupt()}.
   *
   * @throws Exception if master thread cannot be interrupted
   */
  public void kill() throws Exception {
    mMasterThread.interrupt();
  }

  /**
   * Clears all the clients.
   *
   * @throws IOException if the client pool cannot be closed
   */
  public void clearClients() throws IOException {
    mClientPool.close();
  }

  /**
   * @return the externally resolvable address of the master (used by unit test only)
   */
  public InetSocketAddress getAddress() {
    return mTachyonMaster.getMasterAddress();
  }

  /**
   * @return the internal {@link TachyonMaster}
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

  /**
   * @return the URI of the master
   */
  public String getUri() {
    return Constants.HEADER + mHostname + ":" + getRPCLocalPort();
  }

  /**
   * @return the client from the pool
   * @throws IOException if the client cannot be retrieved
   */
  public FileSystem getClient() throws IOException {
    return mClientPool.getClient(ClientContext.getConf());
  }

  private static String uniquePath() throws IOException {
    return File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.nanoTime();
  }

  /**
   * @return the folder of the journal
   */
  public String getJournalFolder() {
    return mJournalFolder;
  }
}
