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
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.io.FileUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Supplier;
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

  private final Supplier<String> mClientSupplier = this::getUri;

  private final ClientPool mClientPool = new ClientPool(mClientSupplier);

  private AlluxioMasterProcess mMasterProcess;
  private Thread mMasterThread;

  private LocalAlluxioMaster() {
    mHostname = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC,
        Configuration.global());
    mJournalFolder = Configuration.getString(PropertyKey.MASTER_JOURNAL_FOLDER);
  }

  /**
   * Creates a new local Alluxio master with an isolated work directory and port.
   *
   * @return an instance of Alluxio master
   */
  public static LocalAlluxioMaster create() throws IOException {
    String workDirectory = uniquePath();
    FileUtils.deletePathRecursively(workDirectory);
    Configuration.set(PropertyKey.WORK_DIR, workDirectory);
    return create(workDirectory);
  }

  /**
   * Creates a new local Alluxio master with a isolated port.
   *
   * @param workDirectory Alluxio work directory, this method will create it if it doesn't exist yet
   * @return the created Alluxio master
   */
  public static LocalAlluxioMaster create(String workDirectory)
      throws IOException {
    if (!Files.isDirectory(Paths.get(workDirectory))) {
      Files.createDirectory(Paths.get(workDirectory));
    }
    return new LocalAlluxioMaster();
  }

  /**
   * Starts the master.
   */
  public void start() {
    mMasterProcess = AlluxioMasterProcess.Factory.create();
    Runnable runMaster = new Runnable() {
      @Override
      public void run() {
        try {
          LOG.info("Starting Alluxio master {}.", mMasterProcess);
          mMasterProcess.start();
        } catch (InterruptedException e) {
          // this is expected
        } catch (Exception e) {
          // Log the exception as the RuntimeException will be caught and handled silently by JUnit
          LOG.error("Start master error", e);
          throw new RuntimeException(e + " \n Start Master Error \n" + e.getMessage(), e);
        }
      }
    };
    mMasterThread = new Thread(runMaster);
    mMasterThread.setName("MasterThread-" + System.identityHashCode(mMasterThread));
    mMasterThread.start();
  }

  /**
   * @return true if the master is serving, false otherwise
   */
  public boolean isServing() {
    return mMasterProcess.isGrpcServingAsLeader();
  }

  /**
   * Stops the master processes and cleans up client connections.
   */
  public void stop() throws Exception {
    if (mMasterThread != null) {
      mMasterProcess.stop();
      while (mMasterThread.isAlive()) {
        LOG.info("Stopping thread {}.", mMasterThread.getName());
        mMasterThread.interrupt();
        mMasterThread.join(1000);
      }
      mMasterThread = null;
    }
    clearClients();
    System.clearProperty("alluxio.web.resources");
    System.clearProperty("alluxio.master.min.worker.threads");
  }

  /**
   * Clears all the clients.
   */
  public void clearClients() throws IOException {
    mClientPool.close();
  }

  /**
   * @return the externally resolvable address of the master
   */
  public InetSocketAddress getAddress() {
    return mMasterProcess.getRpcAddress();
  }

  /**
   * @return the internal {@link MasterProcess}
   */
  public AlluxioMasterProcess getMasterProcess() {
    return mMasterProcess;
  }

  /**
   * Gets the actual port that the RPC service is listening on.
   *
   * @return the RPC local port
   */
  public int getRpcLocalPort() {
    return mMasterProcess.getRpcAddress().getPort();
  }

  /**
   * @return the URI of the master
   */
  public String getUri() {
    return Constants.HEADER + mHostname + ":" + getRpcLocalPort();
  }

  /**
   * @return the client from the pool
   */
  public FileSystem getClient() throws IOException {
    return mClientPool.getClient();
  }

  /**
   * @param context the FileSystemContext to use
   * @return the client from the pool, using a specific context
   */
  public FileSystem getClient(FileSystemContext context) throws IOException {
    return mClientPool.getClient(context);
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
