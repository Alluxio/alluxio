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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.JournalType;
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

  private final boolean mIncludeSecondary;

  private AlluxioMasterProcess mMasterProcess;
  private Thread mMasterThread;

  private AlluxioSecondaryMaster mSecondaryMaster;
  private Thread mSecondaryMasterThread;

  private LocalAlluxioMaster(boolean includeSecondary) {
    mHostname = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC,
        ServerConfiguration.global());
    mJournalFolder = ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    mIncludeSecondary = includeSecondary;
  }

  /**
   * Creates a new local Alluxio master with an isolated work directory and port.
   *
   * @param includeSecondary whether to start a secondary master alongside the regular master
   * @return an instance of Alluxio master
   */
  public static LocalAlluxioMaster create(boolean includeSecondary) throws IOException {
    String workDirectory = uniquePath();
    FileUtils.deletePathRecursively(workDirectory);
    ServerConfiguration.set(PropertyKey.WORK_DIR, workDirectory);
    return create(workDirectory, includeSecondary);
  }

  /**
   * Creates a new local Alluxio master with a isolated port.
   *
   * @param workDirectory Alluxio work directory, this method will create it if it doesn't exist yet
   * @param includeSecondary whether to start a secondary master alongside the regular master
   * @return the created Alluxio master
   */
  public static LocalAlluxioMaster create(String workDirectory, boolean includeSecondary)
      throws IOException {
    if (!Files.isDirectory(Paths.get(workDirectory))) {
      Files.createDirectory(Paths.get(workDirectory));
    }
    return new LocalAlluxioMaster(includeSecondary);
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
    TestUtils.waitForReady(mMasterProcess);
    // Don't start a secondary master when using the Raft journal.
    if (ServerConfiguration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE,
        JournalType.class) == JournalType.EMBEDDED) {
      return;
    }
    if (!mIncludeSecondary) {
      return;
    }
    mSecondaryMaster = new AlluxioSecondaryMaster();
    Runnable runSecondaryMaster = new Runnable() {
      @Override
      public void run() {
        try {
          LOG.info("Starting secondary master {}.", mSecondaryMaster);
          mSecondaryMaster.start();
        } catch (InterruptedException e) {
          // this is expected
        } catch (Exception e) {
          // Log the exception as the RuntimeException will be caught and handled silently by JUnit
          LOG.error("Start secondary master error", e);
          throw new RuntimeException(e + " \n Start Secondary Master Error \n" + e.getMessage(), e);
        }
      }
    };
    mSecondaryMasterThread = new Thread(runSecondaryMaster);
    mSecondaryMasterThread
        .setName("SecondaryMasterThread-" + System.identityHashCode(mSecondaryMasterThread));
    mSecondaryMasterThread.start();
    TestUtils.waitForReady(mSecondaryMaster);
  }

  /**
   * @return true if the master is serving, false otherwise
   */
  public boolean isServing() {
    return mMasterProcess.isServing();
  }

  /**
   * Stops the master processes and cleans up client connections.
   */
  public void stop() throws Exception {
    if (mSecondaryMasterThread != null) {
      mSecondaryMaster.stop();
      while (mSecondaryMasterThread.isAlive()) {
        LOG.info("Stopping thread {}.", mSecondaryMasterThread.getName());
        mSecondaryMasterThread.join(1000);
      }
      mSecondaryMasterThread = null;
    }
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
