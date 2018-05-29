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
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryAssociation;
import alluxio.master.journal.JournalEntryStreamReader;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalSystem.Mode;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.metrics.sink.PrometheusMetricsServlet;
import alluxio.network.thrift.ThriftUtils;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.LockResource;
import alluxio.security.authentication.TransportProvider;
import alluxio.thrift.MetaMasterClientService;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.CommonUtils;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.URIUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.MasterWebServer;
import alluxio.web.WebServer;
import alluxio.wire.ConfigProperty;
import alluxio.wire.ExportJournalOptions;
import alluxio.wire.ExportJournalResponse;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class encapsulates the different master services that are configured to run.
 */
@NotThreadSafe
public class AlluxioMasterProcess implements MasterProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioMasterProcess.class);

  /** Maximum number of threads to serve the rpc server. */
  private final int mMaxWorkerThreads;

  /** Minimum number of threads to serve the rpc server. */
  private final int mMinWorkerThreads;

  /** The port for the RPC server. */
  private final int mPort;

  /**
   * Lock for pausing modifications to master state. Holding the this lock allows a thread to
   * guarantee that no other threads will modify master state.
   */
  private final Lock mPauseStateLock;

  /** The socket for thrift rpc server. */
  private TServerSocket mRpcServerSocket;

  /** The transport provider to create thrift server transport. */
  private final TransportProvider mTransportProvider;

  /** The bind address for the rpc server. */
  private final InetSocketAddress mRpcBindAddress;

  /** The connect address for the rpc server. */
  private final InetSocketAddress mRpcConnectAddress;

  private final MetricsServlet mMetricsServlet = new MetricsServlet(MetricsSystem.METRIC_REGISTRY);
  private final PrometheusMetricsServlet mPMetricsServlet = new PrometheusMetricsServlet(
      MetricsSystem.METRIC_REGISTRY);

  /** The master registry. */
  private final MasterRegistry mRegistry;

  /** The web ui server. */
  private WebServer mWebServer;

  /** The RPC server. */
  private TServer mThriftServer;

  /** The start time for when the master started serving the RPC server. */
  private long mStartTimeMs = -1;

  /** The journal system for writing journal entries and restoring master state. */
  protected final JournalSystem mJournalSystem;

  /** The JVMMonitor Progress. */
  private JvmPauseMonitor mJvmPauseMonitor;

  /** The manager of safe mode state. */
  protected final SafeModeManager mSafeModeManager;

  /**
   * Creates a new {@link AlluxioMasterProcess}.
   */
  AlluxioMasterProcess(JournalSystem journalSystem) {
    mJournalSystem = Preconditions.checkNotNull(journalSystem, "journalSystem");
    mMinWorkerThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MIN);
    mMaxWorkerThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MAX);
    int connectionTimeout = (int) Configuration.getMs(PropertyKey.MASTER_CONNECTION_TIMEOUT_MS);

    Preconditions.checkArgument(mMaxWorkerThreads >= mMinWorkerThreads,
        PropertyKey.MASTER_WORKER_THREADS_MAX + " can not be less than "
            + PropertyKey.MASTER_WORKER_THREADS_MIN);

    if (connectionTimeout > 0) {
      LOG.debug("{} connection timeout[{}] is {}", this, PropertyKey.MASTER_CONNECTION_TIMEOUT_MS,
          connectionTimeout);
    }
    try {
      // Extract the port from the generated socket.
      // When running tests, it is fine to use port '0' so the system will figure out what port to
      // use (any random free port).
      // In a production or any real deployment setup, port '0' should not be used as it will make
      // deployment more complicated.
      if (!Configuration.getBoolean(PropertyKey.TEST_MODE)) {
        Preconditions.checkState(Configuration.getInt(PropertyKey.MASTER_RPC_PORT) > 0,
            this + " rpc port is only allowed to be zero in test mode.");
        Preconditions.checkState(Configuration.getInt(PropertyKey.MASTER_WEB_PORT) > 0,
            this + " web port is only allowed to be zero in test mode.");
      }

      mTransportProvider = TransportProvider.Factory.create();
      mRpcServerSocket = ThriftUtils.createThriftServerSocket(
          NetworkAddressUtils.getBindAddress(ServiceType.MASTER_RPC));
      mPort = ThriftUtils.getThriftPort(mRpcServerSocket);
      // reset master rpc port
      Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(mPort));
      mRpcBindAddress = NetworkAddressUtils.getBindAddress(ServiceType.MASTER_RPC);
      mRpcConnectAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC);

      if (!mJournalSystem.isFormatted()) {
        throw new RuntimeException(
            String.format("Journal %s has not been formatted!", mJournalSystem));
      }
      // Create masters.
      mRegistry = new MasterRegistry();
      mSafeModeManager = new DefaultSafeModeManager();
      MasterContext context = new MasterContext(mJournalSystem, mSafeModeManager);
      mPauseStateLock = context.pauseStateLock();
      MasterUtils.createMasters(mRegistry, context);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ExportJournalResponse exportJournal(ExportJournalOptions options) throws IOException {
    String dir = options.getTargetDirectory();
    UnderFileSystem ufs;
    if (options.isLocalFileSystem()) {
      ufs = UnderFileSystem.Factory.create("/", UnderFileSystemConfiguration.defaults());
      LOG.info("Exporting journal backup to local filesystem in directory {}", dir);
    } else {
      ufs = UnderFileSystem.Factory.createForRoot();
      LOG.info("Exporting journal backup to root UFS in directory {}", dir);
    }
    if (!ufs.isDirectory(dir)) {
      if (!ufs.mkdirs(dir, MkdirsOptions.defaults().setCreateParent(true))) {
        throw new IOException(String.format("Failed to create directory %s", dir));
      }
    }
    String exportFilePath;
    try (LockResource lr = new LockResource(mPauseStateLock)) {
      Instant now = Instant.now();
      String exportFileName = String.format("alluxio-journal-%s-%s.gz",
          DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneId.of("UTC")).format(now),
          now.toEpochMilli());
      exportFilePath = PathUtils.concatPath(dir, exportFileName);
      OutputStream ufsStream = ufs.create(exportFilePath);
      try {
        OutputStream zipStream = new GzipCompressorOutputStream(ufsStream);
        for (Master master : mRegistry.getServers()) {
          Iterator<JournalEntry> it = master.getJournalEntryIterator();
          while (it.hasNext()) {
            it.next().toBuilder().clearSequenceNumber().build().writeDelimitedTo(zipStream);
          }
        }
        // This closes the underlying ufs stream.
        zipStream.close();
      } catch (Throwable t) {
        try {
          ufsStream.close();
        } catch (Throwable t2) {
          LOG.error("Failed to close backup stream to {}", exportFilePath, t2);
          t.addSuppressed(t2);
        }
        try {
          ufs.deleteFile(exportFilePath);
        } catch (Throwable t2) {
          LOG.error("Failed to clean up partially-written backup at {}", exportFilePath, t2);
          t.addSuppressed(t2);
        }
        throw t;
      }
    }
    String rootUfs = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    if (options.isLocalFileSystem()) {
      rootUfs = "file:///";
    }
    String backupUri =
        new AlluxioURI(new AlluxioURI(rootUfs), new AlluxioURI(exportFilePath)).toString();
    return new ExportJournalResponse(backupUri,
        NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC));
  }

  private void initFromBackup(AlluxioURI backup) throws IOException {
    LOG.info("Initializing journal from backup {}", backup);
    int count = 0;
    UnderFileSystem ufs;
    if (URIUtils.isLocalFilesystem(backup.toString())) {
      ufs = UnderFileSystem.Factory.create("/", UnderFileSystemConfiguration.defaults());
    } else {
      ufs = UnderFileSystem.Factory.createForRoot();
    }
    try (UnderFileSystem closeUfs = ufs;
         InputStream ufsIn = ufs.open(backup.getPath());
         GzipCompressorInputStream gzIn = new GzipCompressorInputStream(ufsIn);
         JournalEntryStreamReader reader = new JournalEntryStreamReader(gzIn)) {
      List<Master> masters = mRegistry.getServers();
      JournalEntry entry;
      Map<String, Master> mastersByName = Maps.uniqueIndex(masters, Master::getName);
      while ((entry = reader.readEntry()) != null) {
        String masterName = JournalEntryAssociation.getMasterForEntry(entry);
        Master master = mastersByName.get(masterName);
        master.processJournalEntry(entry);
        try (JournalContext jc = master.createJournalContext()) {
          jc.append(entry);
          count++;
        }
      }
    }
    LOG.info("Imported {} entries from backup", count);
  }

  @Override
  public <T extends Master> T getMaster(Class<T> clazz) {
    return mRegistry.get(clazz);
  }

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcConnectAddress;
  }

  @Override
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  @Override
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
  }

  @Override
  @Nullable
  public InetSocketAddress getWebAddress() {
    if (mWebServer != null) {
      return new InetSocketAddress(mWebServer.getBindHost(), mWebServer.getLocalPort());
    }
    return null;
  }

  @Override
  public boolean isInSafeMode() {
    return mSafeModeManager.isInSafeMode();
  }

  @Override
  public boolean isServing() {
    return mThriftServer != null && mThriftServer.isServing();
  }

  @Override
  public List<ConfigProperty> getConfiguration() {
    List<ConfigProperty> configInfoList = new ArrayList<>();
    String alluxioConfPrefix = "alluxio";
    for (Map.Entry<String, String> entry : Configuration.toMap().entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key.startsWith(alluxioConfPrefix) && value != null) {
        PropertyKey propertyKey = PropertyKey.fromString(key);
        Configuration.Source source = Configuration.getSource(propertyKey);
        String sourceStr;
        if (source == Configuration.Source.SITE_PROPERTY) {
          sourceStr =
              String.format("%s (%s)", source.name(), Configuration.getSitePropertiesFile());
        } else {
          sourceStr = source.name();
        }
        configInfoList.add(new ConfigProperty()
            .setName(key).setValue(entry.getValue()).setSource(sourceStr));
      }
    }
    return configInfoList;
  }

  @Override
  public void waitForReady() {
    CommonUtils.waitFor(this + " to start", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return mThriftServer != null && mThriftServer.isServing() && mWebServer != null
            && mWebServer.getServer().isRunning();
      }
    }, WaitForOptions.defaults().setTimeoutMs(10000));
  }

  @Override
  public void start() throws Exception {
    mJournalSystem.start();
    mJournalSystem.setMode(Mode.PRIMARY);
    startMasters(true);
    startServing();
  }

  @Override
  public void stop() throws Exception {
    if (isServing()) {
      stopServing();
      stopMasters();
      mJournalSystem.stop();
    }
  }

  /**
   * Starts all masters, including block master, FileSystem master, lineage master and additional
   * masters.
   *
   * @param isLeader if the Master is leader
   */
  protected void startMasters(boolean isLeader) {
    try {
      if (isLeader) {
        if (Configuration.containsKey(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP)) {
          AlluxioURI backup =
              new AlluxioURI(Configuration.get(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP));
          if (mJournalSystem.isEmpty()) {
            initFromBackup(backup);
          } else {
            LOG.info("The journal system is not freshly formatted, skipping restoring backup from "
                + backup);
          }
        }
        mSafeModeManager.notifyPrimaryMasterStarted();
      }
      mRegistry.start(isLeader);
      LOG.info("All masters started");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Stops all masters, including lineage master, block master and fileSystem master and additional
   * masters.
   */
  protected void stopMasters() {
    try {
      mRegistry.stop();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void startServing() {
    startServing("", "");
  }

  /**
   * Starts serving, letting {@link MetricsSystem} start sink and starting the web ui server and RPC
   * Server.
   *
   * @param startMessage empty string or the message that the master gains the leadership
   * @param stopMessage empty string or the message that the master loses the leadership
   */
  protected void startServing(String startMessage, String stopMessage) {
    MetricsSystem.startSinks();
    startServingWebServer();
    startJvmMonitorProcess();
    LOG.info("Alluxio master version {} started{}. "
            + "bindHost={}, connectHost={}, rpcPort={}, webPort={}",
        RuntimeConstants.VERSION,
        startMessage,
        NetworkAddressUtils.getBindAddress(ServiceType.MASTER_RPC),
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC),
        NetworkAddressUtils.getPort(ServiceType.MASTER_RPC),
        NetworkAddressUtils.getPort(ServiceType.MASTER_WEB));
    startServingRPCServer();
    LOG.info("Alluxio master ended{}", stopMessage);
  }

  /**
   * Starts serving web ui server, resetting master web port, adding the metrics servlet to the web
   * server and starting web ui.
   */
  protected void startServingWebServer() {
    mWebServer = new MasterWebServer(ServiceType.MASTER_WEB.getServiceName(),
        NetworkAddressUtils.getBindAddress(ServiceType.MASTER_WEB), this);
    // reset master web port
    Configuration.set(PropertyKey.MASTER_WEB_PORT, Integer.toString(mWebServer.getLocalPort()));
    // Add the metrics servlet to the web server.
    mWebServer.addHandler(mMetricsServlet.getHandler());
    // Add the prometheus metrics servlet to the web server.
    mWebServer.addHandler(mPMetricsServlet.getHandler());
    // start web ui
    mWebServer.start();
  }

  /**
   * Starts jvm monitor process, to monitor jvm.
   */
  protected void startJvmMonitorProcess() {
    if (Configuration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED)) {
      mJvmPauseMonitor = new JvmPauseMonitor();
      mJvmPauseMonitor.start();
    }
  }

  private void registerServices(TMultiplexedProcessor processor, Map<String, TProcessor> services) {
    for (Map.Entry<String, TProcessor> service : services.entrySet()) {
      processor.registerProcessor(service.getKey(), service.getValue());
    }
  }

  /**
   * Starts the Thrift RPC server. The AlluxioMaster registers the Services of registered
   * {@link Master}s and meta services to a multiplexed processor, then creates the master thrift
   * service with the multiplexed processor.
   */
  protected void startServingRPCServer() {
    // set up multiplexed thrift processors
    TMultiplexedProcessor processor = new TMultiplexedProcessor();
    // register master services
    for (Master master : mRegistry.getServers()) {
      registerServices(processor, master.getServices());
    }
    // register meta services
    processor.registerProcessor(Constants.META_MASTER_SERVICE_NAME,
        new MetaMasterClientService.Processor<>(new MetaMasterClientServiceHandler(this)));

    // Return a TTransportFactory based on the authentication type
    TTransportFactory transportFactory;
    try {
      String serverName = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC);
      transportFactory = mTransportProvider.getServerTransportFactory(serverName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    try {
      if (mRpcServerSocket == null) {
        mRpcServerSocket = ThriftUtils.createThriftServerSocket(mRpcBindAddress);
      }
    } catch (TTransportException e) {
      throw new RuntimeException(e);
    }
    // create master thrift service with the multiplexed processor.
    Args args = new TThreadPoolServer.Args(mRpcServerSocket)
        .maxWorkerThreads(mMaxWorkerThreads)
        .minWorkerThreads(mMinWorkerThreads)
        .processor(processor)
        .transportFactory(transportFactory)
        .protocolFactory(ThriftUtils.createThriftProtocolFactory())
        .stopTimeoutVal((int) Configuration.getMs(PropertyKey.MASTER_THRIFT_SHUTDOWN_TIMEOUT));
    mThriftServer = new TThreadPoolServer(args);

    // start thrift rpc server
    mStartTimeMs = System.currentTimeMillis();
    mSafeModeManager.notifyRpcServerStarted();
    mThriftServer.serve();
  }

  /**
   * Stops serving, trying stop RPC server and web ui server and letting {@link MetricsSystem} stop
   * all the sinks.
   */
  protected void stopServing() throws Exception {
    if (mThriftServer != null) {
      mThriftServer.stop();
      mThriftServer = null;
    }
    if (mRpcServerSocket != null) {
      mRpcServerSocket.close();
      mRpcServerSocket = null;
    }
    if (mJvmPauseMonitor != null) {
      mJvmPauseMonitor.stop();
    }
    if (mWebServer != null) {
      mWebServer.stop();
      mWebServer = null;
    }
    MetricsSystem.stopSinks();
  }

  @Override
  public String toString() {
    return "Alluxio master @" + mRpcConnectAddress;
  }
}
