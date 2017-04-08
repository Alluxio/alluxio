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
  private static final Logger LOG = LoggerFactory.getLogger(LocalAlluxioMaster.class);

  private final String mHostname;

  private final String mJournalFolder;

  private final Supplier<String> mClientSupplier = new Supplier<String>() {
    @Override
    public String get() {
      return getUri();
    }
  };
  private final ClientPool mClientPool = new ClientPool(mClientSupplier);

  private AlluxioMasterService mAlluxioMaster;
  private Thread mMasterThread;

  private AlluxioSecondaryMaster mSecondaryMaster;
  private Thread mSecondaryMasterThread;

  private LocalAlluxioMaster() throws IOException {
    mHostname = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC);
    mJournalFolder = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
  }

  /**
   * Creates a new local Alluxio master with an isolated work directory and port.
   *
   * @throws IOException when unable to do file operation or listen on port
   * @return an instance of Alluxio master
   */
  public static LocalAlluxioMaster create() throws IOException {
    String workDirectory = uniquePath();
    UnderFileSystemUtils.deleteDirIfExists(workDirectory);
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
    mAlluxioMaster = AlluxioMasterService.Factory.create();
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
    mMasterThread.start();
  }

  /**
   * Starts the secondary master.
   */
  public void startSecondary() {
    mSecondaryMaster = new AlluxioSecondaryMaster();
    Runnable runSecondaryMaster = new Runnable() {
      @Override
      public void run() {
        try {
          mSecondaryMaster.start();
        } catch (Exception e) {
          // Log the exception as the RuntimeException will be caught and handled silently by JUnit
          LOG.error("Start secondary master error", e);
          throw new RuntimeException(e + " \n Start Secondary Master Error \n" + e.getMessage(), e);
        }
      }
    };

    mSecondaryMasterThread = new Thread(runSecondaryMaster);
    mSecondaryMasterThread.start();
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
    mMasterThread.interrupt();
    if (mSecondaryMaster != null) {
      mSecondaryMaster.stop();
    }
    if (mSecondaryMasterThread != null) {
      mSecondaryMasterThread.interrupt();
    }

    System.clearProperty("alluxio.web.resources");
    System.clearProperty("alluxio.master.min.worker.threads");
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
   * @return the externally resolvable address of the master
   */
  public InetSocketAddress getAddress() {
    return mAlluxioMaster.getRpcAddress();
  }

  /**
   * @return the internal {@link AlluxioMasterService}
   */
  public AlluxioMasterService getInternalMaster() {
    return mAlluxioMaster;
  }

  /**
   * Gets the actual port that the RPC service is listening on.
   *
   * @return the RPC local port
   */
  public int getRpcLocalPort() {
    return mAlluxioMaster.getRpcAddress().getPort();
  }

  /**
   * @return the URI of the master
   */
  public String getUri() {
    return Constants.HEADER + mHostname + ":" + getRpcLocalPort();
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
