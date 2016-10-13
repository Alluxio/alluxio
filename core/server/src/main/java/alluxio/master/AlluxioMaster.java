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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.Server;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.master.lineage.LineageMaster;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.security.authentication.TransportProvider;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.LineageUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.MasterUIWebServer;
import alluxio.web.UIWebServer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Entry point for the Alluxio master program.
 */
@NotThreadSafe
public class AlluxioMaster implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Starts the Alluxio master.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioMaster.class.getCanonicalName());
      System.exit(-1);
    }

    // validate the configuration
    if (!ConfigurationUtils.validateConf()) {
      LOG.error("Invalid configuration found");
      System.exit(-1);
    }

    AlluxioMaster master = Factory.create();
    try {
      master.start();
    } catch (Exception e) {
      LOG.error("Uncaught exception while running Alluxio master, stopping it and exiting.", e);
      try {
        master.stop();
      } catch (Exception e2) {
        // continue to exit
        LOG.error("Uncaught exception while stopping Alluxio master, simply exiting.", e2);
      }
      System.exit(-1);
    }
  }

  /** Maximum number of threads to serve the rpc server. */
  private final int mMaxWorkerThreads;

  /** Minimum number of threads to serve the rpc server. */
  private final int mMinWorkerThreads;

  /** The port for the RPC server. */
  private final int mPort;

  /** The socket for thrift rpc server. */
  private final TServerSocket mTServerSocket;

  /** The transport provider to create thrift server transport. */
  private final TransportProvider mTransportProvider;

  /** The address for the rpc server. */
  private final InetSocketAddress mMasterAddress;

  private final MetricsServlet mMetricsServlet = new MetricsServlet(MetricsSystem.METRIC_REGISTRY);

  /** The master managing all block metadata. */
  protected BlockMaster mBlockMaster;

  /** The master managing all file system related metadata. */
  protected FileSystemMaster mFileSystemMaster;

  /** The master managing all lineage related metadata. */
  protected LineageMaster mLineageMaster;

  /** A list of extra masters to launch based on service loader. */
  protected List<Master> mAdditionalMasters;

  /** The journal for the block master. */
  protected final ReadWriteJournal mBlockMasterJournal;

  /** The journal for the file system master. */
  protected final ReadWriteJournal mFileSystemMasterJournal;

  /** The journal for the lineage master. */
  protected final ReadWriteJournal mLineageMasterJournal;

  /** The web ui server. */
  private UIWebServer mWebServer = null;

  /** The RPC server. */
  private TServer mMasterServiceServer = null;

  /** is true if the master is serving the RPC server. */
  private boolean mIsServing = false;

  /** The start time for when the master started serving the RPC server. */
  private long mStartTimeMs = -1;

  /** The master services' names. */
  private static List<String> sServiceNames;

  /** The master service loaders. */
  private static ServiceLoader<MasterFactory> sServiceLoader;

  /**
   * @return the (cached) master service loader
   */
  private static ServiceLoader<MasterFactory> getServiceLoader() {
    if (sServiceLoader != null) {
      return sServiceLoader;
    }
    // Discover and register the available factories.
    // NOTE: ClassLoader is explicitly specified so we don't need to set ContextClassLoader.
    sServiceLoader = ServiceLoader.load(MasterFactory.class, MasterFactory.class.getClassLoader());
    return sServiceLoader;
  }

  /**
   * @return the (cached) list of the enabled master services' names
   */
  public static List<String> getServiceNames() {
    if (sServiceNames != null) {
      return sServiceNames;
    }
    sServiceNames = new ArrayList<>();
    sServiceNames.add(Constants.BLOCK_MASTER_NAME);
    sServiceNames.add(Constants.FILE_SYSTEM_MASTER_NAME);
    sServiceNames.add(Constants.LINEAGE_MASTER_NAME);

    for (MasterFactory factory : getServiceLoader()) {
      if (factory.isEnabled()) {
        sServiceNames.add(factory.getName());
      }
    }

    return sServiceNames;
  }

  /**
   * Factory for creating {@link AlluxioMaster} or {@link FaultTolerantAlluxioMaster} based on
   * {@link Configuration}.
   */
  @ThreadSafe
  public static final class Factory {
    /**
     * @return {@link FaultTolerantAlluxioMaster} if Alluxio configuration is set to use zookeeper,
     *         otherwise, return {@link AlluxioMaster}.
     */
    public static AlluxioMaster create() {
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return new FaultTolerantAlluxioMaster();
      }
      return new AlluxioMaster();
    }

    private Factory() {} // prevent instantiation.
  }

  protected AlluxioMaster() {
    mMinWorkerThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MIN);
    mMaxWorkerThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MAX);

    Preconditions.checkArgument(mMaxWorkerThreads >= mMinWorkerThreads,
        PropertyKey.MASTER_WORKER_THREADS_MAX + " can not be less than "
            + PropertyKey.MASTER_WORKER_THREADS_MIN);

    try {
      // Extract the port from the generated socket.
      // When running tests, it is fine to use port '0' so the system will figure out what port to
      // use (any random free port).
      // In a production or any real deployment setup, port '0' should not be used as it will make
      // deployment more complicated.
      if (!Configuration.getBoolean(PropertyKey.TEST_MODE)) {
        Preconditions.checkState(Configuration.getInt(PropertyKey.MASTER_RPC_PORT) > 0,
            "Alluxio master rpc port is only allowed to be zero in test mode.");
        Preconditions.checkState(Configuration.getInt(PropertyKey.MASTER_WEB_PORT) > 0,
            "Alluxio master web port is only allowed to be zero in test mode.");
      }
      mTransportProvider = TransportProvider.Factory.create();
      mTServerSocket =
          new TServerSocket(NetworkAddressUtils.getBindAddress(ServiceType.MASTER_RPC));
      mPort = NetworkAddressUtils.getThriftPort(mTServerSocket);
      // reset master rpc port
      Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(mPort));
      mMasterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC);

      // Check the journal directory
      String journalDirectory = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
      if (!journalDirectory.endsWith(AlluxioURI.SEPARATOR)) {
        journalDirectory += AlluxioURI.SEPARATOR;
      }
      Preconditions.checkState(isJournalFormatted(journalDirectory),
          "Alluxio master was not formatted! The journal folder is " + journalDirectory);

      // Create the journals.
      mBlockMasterJournal = new ReadWriteJournal(BlockMaster.getJournalDirectory(journalDirectory));
      mFileSystemMasterJournal =
          new ReadWriteJournal(FileSystemMaster.getJournalDirectory(journalDirectory));
      mLineageMasterJournal =
          new ReadWriteJournal(LineageMaster.getJournalDirectory(journalDirectory));

      mBlockMaster = new BlockMaster(mBlockMasterJournal);
      mFileSystemMaster = new FileSystemMaster(mBlockMaster, mFileSystemMasterJournal);
      if (LineageUtils.isLineageEnabled()) {
        mLineageMaster = new LineageMaster(mFileSystemMaster, mLineageMasterJournal);
      }

      mAdditionalMasters = new ArrayList<>();
      List<? extends Master> masters = Lists.newArrayList(mBlockMaster, mFileSystemMaster);
      for (MasterFactory factory : getServiceLoader()) {
        Master master = factory.create(masters, journalDirectory);
        if (master != null) {
          mAdditionalMasters.add(master);
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * @return the externally resolvable address of this master
   */
  public InetSocketAddress getMasterAddress() {
    return mMasterAddress;
  }

  /**
   * @return the actual bind hostname on RPC service (used by unit test only)
   */
  public String getRPCBindHost() {
    return NetworkAddressUtils.getThriftSocket(mTServerSocket).getInetAddress().getHostAddress();
  }

  /**
   * @return the actual port that the RPC service is listening on (used by unit test only)
   */
  public int getRPCLocalPort() {
    return mPort;
  }

  /**
   * @return the actual bind hostname on web service (used by unit test only)
   */
  public String getWebBindHost() {
    if (mWebServer != null) {
      return mWebServer.getBindHost();
    }
    return "";
  }

  /**
   * @return the actual port that the web service is listening on (used by unit test only)
   */
  public int getWebLocalPort() {
    if (mWebServer != null) {
      return mWebServer.getLocalPort();
    }
    return -1;
  }

  /**
   * @return internal {@link BlockMaster}
   */
  public BlockMaster getBlockMaster() {
    return mBlockMaster;
  }

  /**
   * @return other additional {@link Master}s
   */
  public List<Master> getAdditionalMasters() {
    return Collections.unmodifiableList(mAdditionalMasters);
  }

  /**
   * @return internal {@link FileSystemMaster}
   */
  public FileSystemMaster getFileSystemMaster() {
    return mFileSystemMaster;
  }

  /**
   * @return internal {@link LineageMaster}
   */
  public LineageMaster getLineageMaster() {
    return mLineageMaster;
  }

  /**
   * @return the start time of the master in milliseconds
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the uptime of the master in milliseconds
   */
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
  }

  /**
   * @return true if the system is the leader (serving the rpc server), false otherwise
   */
  boolean isServing() {
    return mIsServing;
  }

  @Override
  public void start() throws Exception {
    startMasters(true);
    startServing();
  }

  @Override
  public void stop() throws Exception {
    if (mIsServing) {
      LOG.info("Stopping RPC server on Alluxio master @ {}", mMasterAddress);
      stopServing();
      stopMasters();
      mTServerSocket.close();
      mIsServing = false;
    } else {
      LOG.info("Stopping Alluxio master @ {}", mMasterAddress);
    }
  }

  protected void startMasters(boolean isLeader) {
    try {
      connectToUFS();

      mBlockMaster.start(isLeader);
      mFileSystemMaster.start(isLeader);
      if (LineageUtils.isLineageEnabled()) {
        mLineageMaster.start(isLeader);
      }
      // start additional masters
      for (Master master : mAdditionalMasters) {
        master.start(isLeader);
      }

    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  protected void stopMasters() {
    try {
      if (LineageUtils.isLineageEnabled()) {
        mLineageMaster.stop();
      }
      // stop additional masters
      for (Master master : mAdditionalMasters) {
        master.stop();
      }
      mBlockMaster.stop();
      mFileSystemMaster.stop();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  private void startServing() {
    startServing("", "");
  }

  protected void startServing(String startMessage, String stopMessage) {
    MetricsSystem.startSinks();
    startServingWebServer();
    LOG.info("Alluxio master version {} started @ {} {}", RuntimeConstants.VERSION, mMasterAddress,
        startMessage);
    startServingRPCServer();
    LOG.info("Alluxio master version {} ended @ {} {}", RuntimeConstants.VERSION, mMasterAddress,
        stopMessage);
  }

  protected void startServingWebServer() {
    mWebServer = new MasterUIWebServer(ServiceType.MASTER_WEB.getServiceName(),
        NetworkAddressUtils.getBindAddress(ServiceType.MASTER_WEB), this);
    // reset master web port
    Configuration.set(PropertyKey.MASTER_WEB_PORT, Integer.toString(mWebServer.getLocalPort()));
    // Add the metrics servlet to the web server.
    mWebServer.addHandler(mMetricsServlet.getHandler());
    // start web ui
    mWebServer.startWebServer();
  }

  private void registerServices(TMultiplexedProcessor processor, Map<String, TProcessor> services) {
    for (Map.Entry<String, TProcessor> service : services.entrySet()) {
      processor.registerProcessor(service.getKey(), service.getValue());
    }
  }

  protected void startServingRPCServer() {
    // set up multiplexed thrift processors
    TMultiplexedProcessor processor = new TMultiplexedProcessor();
    registerServices(processor, mBlockMaster.getServices());
    registerServices(processor, mFileSystemMaster.getServices());
    if (LineageUtils.isLineageEnabled()) {
      registerServices(processor, mLineageMaster.getServices());
    }
    // register additional masters for RPC service
    for (Master master : mAdditionalMasters) {
      registerServices(processor, master.getServices());
    }

    // Return a TTransportFactory based on the authentication type
    TTransportFactory transportFactory;
    try {
      transportFactory = mTransportProvider.getServerTransportFactory();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    // create master thrift service with the multiplexed processor.
    Args args = new TThreadPoolServer.Args(mTServerSocket).maxWorkerThreads(mMaxWorkerThreads)
        .minWorkerThreads(mMinWorkerThreads).processor(processor).transportFactory(transportFactory)
        .protocolFactory(new TBinaryProtocol.Factory(true, true));
    if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
      args.stopTimeoutVal = 0;
    } else {
      args.stopTimeoutVal = Constants.THRIFT_STOP_TIMEOUT_SECONDS;
    }
    mMasterServiceServer = new TThreadPoolServer(args);

    // start thrift rpc server
    mIsServing = true;
    mStartTimeMs = System.currentTimeMillis();
    mMasterServiceServer.serve();
  }

  protected void stopServing() throws Exception {
    if (mMasterServiceServer != null) {
      mMasterServiceServer.stop();
      mMasterServiceServer = null;
    }
    if (mWebServer != null) {
      mWebServer.shutdownWebServer();
      mWebServer = null;
    }
    MetricsSystem.stopSinks();
    mIsServing = false;
  }

  /**
   * Checks to see if the journal directory is formatted.
   *
   * @param journalDirectory the journal directory to check
   * @return true if the journal directory was formatted previously, false otherwise
   * @throws IOException if an I/O error occurs
   */
  private boolean isJournalFormatted(String journalDirectory) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(journalDirectory);
    if (!ufs.providesStorage()) {
      // TODO(gene): Should the journal really be allowed on a ufs without storage?
      // This ufs doesn't provide storage. Allow the master to use this ufs for the journal.
      LOG.info("Journal directory doesn't provide storage: {}", journalDirectory);
      return true;
    }
    String[] files = ufs.list(journalDirectory);
    if (files == null) {
      return false;
    }
    // Search for the format file.
    String formatFilePrefix = Configuration.get(PropertyKey.MASTER_FORMAT_FILE_PREFIX);
    for (String file : files) {
      if (file.startsWith(formatFilePrefix)) {
        return true;
      }
    }
    return false;
  }

  private void connectToUFS() throws IOException {
    String ufsAddress = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
    UnderFileSystem ufs = UnderFileSystem.get(ufsAddress);
    ufs.connectFromMaster(NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC));
  }

  /**
   * Blocks until the master is ready to serve requests.
   */
  public void waitForReady() {
    while (true) {
      if (mMasterServiceServer != null && mMasterServiceServer.isServing()
          && mWebServer != null && mWebServer.getServer().isRunning()) {
        return;
      }
      CommonUtils.sleepMs(10);
    }
  }
}
