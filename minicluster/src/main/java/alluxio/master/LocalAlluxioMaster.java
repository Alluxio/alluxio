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

package alluxio.master;

import alluxio.AlluxioTestDirectory;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Constructs an isolated master. Primary users of this class are the {@link LocalAlluxioCluster}
 * and {@link MultiMasterLocalAlluxioCluster}.
 *
 * Isolated is defined as having its own root directory, and port.
 */
@NotThreadSafe
public final class LocalAlluxioMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final String mHostname;

  private final String mJournalFolder;

  private final AlluxioMaster mAlluxioMaster;
  private final Thread mMasterThread;

  private final Supplier<String> mClientSupplier = new Supplier<String>() {
    @Override
    public String get() {
      return getUri();
    }
  };
  private final ClientPool mClientPool = new ClientPool(mClientSupplier);

  private LocalAlluxioMaster() throws IOException {
    mHostname = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC);
    mJournalFolder = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    mAlluxioMaster = AlluxioMaster.Factory.create();

    Runnable runMaster = new Runnable() {
      @Override
      public void run() {
        try {
          mAlluxioMaster.start();
        } catch (Exception e) {
          // Log the exception as the RuntimeException will be caught and handled silently by JUnit
          LOG.error("Start master error", e);
          throw new RuntimeException(e + " \n Start Master Error \n" + e.getMessage(), e);
        }
      }
    };

    mMasterThread = new Thread(runMaster);
  }

  /**
   * Creates a new local Alluxio master with an isolated work directory and port.
   *
   * @throws IOException when unable to do file operation or listen on port
   * @return an instance of Alluxio master
   */
  public static LocalAlluxioMaster create() throws IOException {
    String workDirectory = uniquePath();
    UnderFileSystemUtils.deleteDir(workDirectory);
    UnderFileSystemUtils.mkdirIfNotExists(workDirectory);

    Configuration.set(PropertyKey.WORK_DIR, workDirectory);

    return new LocalAlluxioMaster();
  }

  /**
   * Creates a new local Alluxio master with a isolated port.
   *
   * @param workDirectory Alluxio work directory, this method will create it if it doesn't exist yet
   * @return the created Alluxio master
   * @throws IOException when unable to do file operation or listen on port
   */
  public static LocalAlluxioMaster create(final String workDirectory) throws IOException {
    UnderFileSystemUtils.mkdirIfNotExists(workDirectory);

    return new LocalAlluxioMaster();
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
    return mAlluxioMaster.isServing();
  }

  /**
   * Stops the master and cleans up client connections.
   *
   * @throws Exception when the operation fails
   */
  public void stop() throws Exception {
    clearClients();

    mAlluxioMaster.stop();

    System.clearProperty("alluxio.web.resources");
    System.clearProperty("alluxio.master.min.worker.threads");

  }

  /**
   * Kills the master thread, by calling {@link Thread#interrupt()}.
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
    return mAlluxioMaster.getMasterAddress();
  }

  /**
   * @return the internal {@link AlluxioMaster}
   */
  public AlluxioMaster getInternalMaster() {
    return mAlluxioMaster;
  }

  /**
   * Gets the actual bind hostname on RPC service (used by unit test only).
   *
   * @return the RPC bind hostname
   */
  public String getRPCBindHost() {
    return mAlluxioMaster.getRPCBindHost();
  }

  /**
   * Gets the actual port that the RPC service is listening on (used by unit test only).
   *
   * @return the RPC local port
   */
  public int getRPCLocalPort() {
    return mAlluxioMaster.getRPCLocalPort();
  }

  /**
   * Gets the actual bind hostname on web service (used by unit test only).
   *
   * @return the Web bind hostname
   */
  public String getWebBindHost() {
    return mAlluxioMaster.getWebBindHost();
  }

  /**
   * Gets the actual port that the web service is listening on (used by unit test only).
   *
   * @return the Web local port
   */
  public int getWebLocalPort() {
    return mAlluxioMaster.getWebLocalPort();
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
    return mClientPool.getClient();
  }

  private static String uniquePath() throws IOException {
    return AlluxioTestDirectory.createTemporaryDirectory("alluxio-master").getAbsolutePath();
  }

  /**
   * @return the folder of the journal
   */
  public String getJournalFolder() {
    return mJournalFolder;
  }
}
