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

package alluxio.master;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.Version;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.master.lineage.LineageMaster;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authentication.AuthenticationUtils;
import alluxio.underfs.UnderFileSystem;
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
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Entry point for the Alluxio master program.
 */
@NotThreadSafe
public class AlluxioMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static AlluxioMaster sAlluxioMaster = null;

  /**
   * Starts the Alluxio master server via {@code java -cp <ALLUXIO-VERSION> alluxio.Master}.
   *
   * @param args there are no arguments used
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} alluxio.Master", Version.ALLUXIO_JAR);
      System.exit(-1);
    }

    try {
      AlluxioMaster master = get();
      master.start();
    } catch (Exception e) {
      LOG.error("Uncaught exception terminating Master", e);
      System.exit(-1);
    }
  }

  /**
   * Returns a handle to the Alluxio master instance.
   *
   * @return Alluxio master handle
   */
  public static synchronized AlluxioMaster get() {
    if (sAlluxioMaster == null) {
      sAlluxioMaster = Factory.create();
    }
    return sAlluxioMaster;
  }

  /** Maximum number of threads to serve the rpc server. */
  private final int mMaxWorkerThreads;

  /** Minimum number of threads to serve the rpc server. */
  private final int mMinWorkerThreads;

  /** The port for the RPC server. */
  private final int mPort;

  /** The socket for thrift rpc server. */
  private final TServerSocket mTServerSocket;

  /** The address for the rpc server. */
  private final InetSocketAddress mMasterAddress;

  /** The master metrics system. */
  private final MetricsSystem mMasterMetricsSystem;

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
    sServiceNames = Lists.newArrayList();
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
      if (MasterContext.getConf().getBoolean(Constants.ZOOKEEPER_ENABLED)) {
        return new FaultTolerantAlluxioMaster();
      }
      return new AlluxioMaster();
    }

    private Factory() {} // prevent instantiation.
  }

  protected AlluxioMaster() {
    Configuration conf = MasterContext.getConf();

    mMinWorkerThreads = conf.getInt(Constants.MASTER_WORKER_THREADS_MIN);
    mMaxWorkerThreads = conf.getInt(Constants.MASTER_WORKER_THREADS_MAX);

    Preconditions.checkArgument(mMaxWorkerThreads >= mMinWorkerThreads,
        Constants.MASTER_WORKER_THREADS_MAX + " can not be less than "
            + Constants.MASTER_WORKER_THREADS_MIN);

    try {
      // Extract the port from the generated socket.
      // When running tests, it is fine to use port '0' so the system will figure out what port to
      // use (any random free port).
      // In a production or any real deployment setup, port '0' should not be used as it will make
      // deployment more complicated.
      if (!conf.getBoolean(Constants.IN_TEST_MODE)) {
        Preconditions.checkState(conf.getInt(Constants.MASTER_RPC_PORT) > 0,
            "Master rpc port is only allowed to be zero in test mode.");
        Preconditions.checkState(conf.getInt(Constants.MASTER_WEB_PORT) > 0,
            "Master web port is only allowed to be zero in test mode.");
      }
      mTServerSocket =
          new TServerSocket(NetworkAddressUtils.getBindAddress(ServiceType.MASTER_RPC, conf));
      mPort = NetworkAddressUtils.getThriftPort(mTServerSocket);
      // reset master port
      conf.set(Constants.MASTER_RPC_PORT, Integer.toString(mPort));
      mMasterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);

      // Check the journal directory
      String journalDirectory = conf.get(Constants.MASTER_JOURNAL_FOLDER);
      if (!journalDirectory.endsWith(AlluxioURI.SEPARATOR)) {
        journalDirectory += AlluxioURI.SEPARATOR;
      }
      Preconditions.checkState(isJournalFormatted(journalDirectory),
          "Alluxio was not formatted! The journal folder is " + journalDirectory);

      // Create the journals.
      mBlockMasterJournal = new ReadWriteJournal(BlockMaster.getJournalDirectory(journalDirectory));
      mFileSystemMasterJournal =
          new ReadWriteJournal(FileSystemMaster.getJournalDirectory(journalDirectory));
      mLineageMasterJournal =
          new ReadWriteJournal(LineageMaster.getJournalDirectory(journalDirectory));

      mBlockMaster = new BlockMaster(mBlockMasterJournal);
      mFileSystemMaster = new FileSystemMaster(mBlockMaster, mFileSystemMasterJournal);
      if (LineageUtils.isLineageEnabled(MasterContext.getConf())) {
        mLineageMaster = new LineageMaster(mFileSystemMaster, mLineageMasterJournal);
      }

      mAdditionalMasters = Lists.newArrayList();
      List<? extends  Master> masters = Lists.newArrayList(mBlockMaster, mFileSystemMaster);
      for (MasterFactory factory : getServiceLoader()) {
        Master master = factory.create(masters, journalDirectory);
        if (master != null) {
          mAdditionalMasters.add(master);
        }
      }

      MasterContext.getMasterSource().registerGauges(this);
      mMasterMetricsSystem = new MetricsSystem("master", MasterContext.getConf());
      mMasterMetricsSystem.registerSource(MasterContext.getMasterSource());
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
   * @return the millisecond when Alluxio Master starts serving, return -1 when not started
   */
  public long getStarttimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return true if the system is the leader (serving the rpc server), false otherwise
   */
  boolean isServing() {
    return mIsServing;
  }

  /**
   * Starts the Alluxio master server.
   *
   * @throws Exception if starting the master fails
   */
  public void start() throws Exception {
    startMasters(true);
    startServing();
  }

  /**
   * Stops the Alluxio master server.
   *
   * @throws Exception if stopping the master fails
   */
  public void stop() throws Exception {
    if (mIsServing) {
      LOG.info("Stopping RPC server on Alluxio Master @ {}", mMasterAddress);
      stopServing();
      stopMasters();
      mTServerSocket.close();
      mIsServing = false;
    } else {
      LOG.info("Stopping Alluxio Master @ {}", mMasterAddress);
    }
  }

  protected void startMasters(boolean isLeader) {
    try {
      connectToUFS();

      mBlockMaster.start(isLeader);
      mFileSystemMaster.start(isLeader);
      if (LineageUtils.isLineageEnabled(MasterContext.getConf())) {
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
      if (LineageUtils.isLineageEnabled(MasterContext.getConf())) {
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
    mMasterMetricsSystem.start();
    startServingWebServer();
    LOG.info("Alluxio Master version {} started @ {} {}", Version.VERSION, mMasterAddress,
        startMessage);
    startServingRPCServer();
    LOG.info("Alluxio Master version {} ended @ {} {}", Version.VERSION, mMasterAddress,
        stopMessage);
  }

  protected void startServingWebServer() {
    Configuration conf = MasterContext.getConf();
    mWebServer =
        new MasterUIWebServer(ServiceType.MASTER_WEB, NetworkAddressUtils.getBindAddress(
            ServiceType.MASTER_WEB, conf), this, conf);

    // Add the metrics servlet to the web server, this must be done after the metrics system starts
    mWebServer.addHandler(mMasterMetricsSystem.getServletHandler());
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
    if (LineageUtils.isLineageEnabled(MasterContext.getConf())) {
      registerServices(processor, mLineageMaster.getServices());
    }
    // register additional masters for RPC service
    for (Master master : mAdditionalMasters) {
      registerServices(processor, master.getServices());
    }

    // Return a TTransportFactory based on the authentication type
    TTransportFactory transportFactory;
    try {
      transportFactory = AuthenticationUtils.getServerTransportFactory(MasterContext.getConf());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    // create master thrift service with the multiplexed processor.
    Args args = new TThreadPoolServer.Args(mTServerSocket).maxWorkerThreads(mMaxWorkerThreads)
        .minWorkerThreads(mMinWorkerThreads).processor(processor).transportFactory(transportFactory)
        .protocolFactory(new TBinaryProtocol.Factory(true, true));
    if (MasterContext.getConf().getBoolean(Constants.IN_TEST_MODE)) {
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
    mMasterMetricsSystem.stop();
    mIsServing = false;
  }

  /**
   * Checks to see if the journal directory is formatted.
   *
   * @param journalDirectory The journal directory to check
   * @return true if the journal directory was formatted previously, false otherwise
   * @throws IOException if an I/O error occurs
   */
  private boolean isJournalFormatted(String journalDirectory) throws IOException {
    Configuration conf = MasterContext.getConf();
    UnderFileSystem ufs = UnderFileSystem.get(journalDirectory, conf);
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
    String formatFilePrefix = conf.get(Constants.MASTER_FORMAT_FILE_PREFIX);
    for (String file : files) {
      if (file.startsWith(formatFilePrefix)) {
        return true;
      }
    }
    return false;
  }

  private void connectToUFS() throws IOException {
    Configuration conf = MasterContext.getConf();
    String ufsAddress = conf.get(Constants.UNDERFS_ADDRESS);
    UnderFileSystem ufs = UnderFileSystem.get(ufsAddress, conf);
    ufs.connectFromMaster(conf, NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC, conf));
  }

  /**
   * @return the master metric system reference
   */
  public MetricsSystem getMasterMetricsSystem() {
    return mMasterMetricsSystem;
  }
}
