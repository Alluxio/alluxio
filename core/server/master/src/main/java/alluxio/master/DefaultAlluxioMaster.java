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
import alluxio.ServerUtils;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalFactory;
import alluxio.master.journal.MutableJournal;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.security.authentication.TransportProvider;
import alluxio.thrift.MetaMasterClientService;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.MasterWebServer;
import alluxio.web.WebServer;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class encapsulates the different master services that are configured to run.
 */
@NotThreadSafe
public class DefaultAlluxioMaster implements AlluxioMasterService {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultAlluxioMaster.class);

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
  private final InetSocketAddress mRpcAddress;

  private final MetricsServlet mMetricsServlet = new MetricsServlet(MetricsSystem.METRIC_REGISTRY);

  /** The master registry. */
  protected MasterRegistry mRegistry;

  /** The web ui server. */
  private WebServer mWebServer = null;

  /** The RPC server. */
  private TServer mMasterServiceServer = null;

  /** is true if the master is serving the RPC server. */
  private boolean mIsServing = false;

  /** The start time for when the master started serving the RPC server. */
  private long mStartTimeMs = -1;

  /**
   * Creates a {@link DefaultAlluxioMaster} by the classes in the same packet of
   * {@link DefaultAlluxioMaster} or the subclasses of {@link DefaultAlluxioMaster}.
   */
  protected DefaultAlluxioMaster() {
    mMinWorkerThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MIN);
    mMaxWorkerThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MAX);
    int connectionTimeout = Configuration.getInt(PropertyKey.MASTER_CONNECTION_TIMEOUT_MS);

    Preconditions.checkArgument(mMaxWorkerThreads >= mMinWorkerThreads,
        PropertyKey.MASTER_WORKER_THREADS_MAX + " can not be less than "
            + PropertyKey.MASTER_WORKER_THREADS_MIN);

    if (connectionTimeout > 0) {
      LOG.debug("Alluxio master connection timeout["
              + PropertyKey.MASTER_CONNECTION_TIMEOUT_MS + "] is " + connectionTimeout);
    }
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
          new TServerSocket(NetworkAddressUtils.getBindAddress(ServiceType.MASTER_RPC),
                  connectionTimeout);
      mPort = NetworkAddressUtils.getThriftPort(mTServerSocket);
      // reset master rpc port
      Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(mPort));
      mRpcAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC);

      // Check that journals of each service have been formatted.
      checkJournalFormatted();
      // Create masters.
      createMasters(new MutableJournal.Factory(getJournalLocation()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Checks whether the journal has been formatted.
   *
   * @throws IOException if the journal has not been formatted
   */
  protected void checkJournalFormatted() throws IOException {
    Journal.Factory factory = new Journal.Factory(getJournalLocation());
    for (String name : ServerUtils.getMasterServiceNames()) {
      Journal journal = factory.create(name);
      if (!journal.isFormatted()) {
        throw new RuntimeException(
            String.format("Journal %s has not been formatted!", journal.getLocation()));
      }
    }
  }

  /**
   * @return the journal location
   */
  protected URI getJournalLocation() {
    String journalDirectory = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    if (!journalDirectory.endsWith(AlluxioURI.SEPARATOR)) {
      journalDirectory += AlluxioURI.SEPARATOR;
    }
    try {
      return new URI(journalDirectory);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param journalFactory the factory to use for creating journals
   */
  protected void createMasters(final JournalFactory journalFactory) {
    mRegistry = new MasterRegistry();
    List<Callable<Void>> callables = new ArrayList<>();
    for (final MasterFactory factory : ServerUtils.getMasterServiceLoader()) {
      callables.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          factory.create(mRegistry, journalFactory);
          return null;
        }
      });
    }
    try {
      Executors.newCachedThreadPool().invokeAll(callables, 10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> T getMaster(Class<T> clazz) {
    return mRegistry.get(clazz);
  }

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcAddress;
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
  public InetSocketAddress getWebAddress() {
    if (mWebServer != null) {
      return new InetSocketAddress(mWebServer.getBindHost(), mWebServer.getLocalPort());
    }
    return null;
  }

  @Override
  public boolean isServing() {
    return mIsServing;
  }

  @Override
  public void waitForReady() {
    CommonUtils.waitFor("Alluxio master to start", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return mMasterServiceServer != null && mMasterServiceServer.isServing()
            && mWebServer != null && mWebServer.getServer().isRunning();
      }
    });
  }

  @Override
  public void start() throws Exception {
    startMasters(true);
    startServing();
  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping Alluxio master @ {}", mRpcAddress);
    if (mIsServing) {
      stopServing();
      stopMasters();
      mTServerSocket.close();
      mIsServing = false;
    }
  }

  /**
   * First establish a connection to the under file system from master, then starts all masters,
   * including block master, FileSystem master, lineage master and additional masters.
   *
   * @param isLeader if the Master is leader
   */
  protected void startMasters(boolean isLeader) {
    try {
      connectToUFS();
      mRegistry.start(isLeader);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Stops all masters, including lineage master, block master and fileSystem master and
   * additional masters.
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
   * Starts serving, letting {@link MetricsSystem} start sink and starting the web ui server and
   * RPC Server.
   *
   * @param startMessage empty string or the message that the master gains the leadership
   * @param stopMessage empty string or the message that the master loses the leadership
   */
  protected void startServing(String startMessage, String stopMessage) {
    MetricsSystem.startSinks();
    startServingWebServer();
    LOG.info("Alluxio master version {} started @ {} {}", RuntimeConstants.VERSION, mRpcAddress,
        startMessage);
    startServingRPCServer();
    LOG.info("Alluxio master version {} ended @ {} {}", RuntimeConstants.VERSION, mRpcAddress,
        stopMessage);
  }

  /**
   * Starts serving web ui server, resetting master web port, adding the metrics servlet to the
   * web server and starting web ui.
   */
  protected void startServingWebServer() {
    mWebServer = new MasterWebServer(ServiceType.MASTER_WEB.getServiceName(),
        NetworkAddressUtils.getBindAddress(ServiceType.MASTER_WEB), this);
    // reset master web port
    Configuration.set(PropertyKey.MASTER_WEB_PORT, Integer.toString(mWebServer.getLocalPort()));
    // Add the metrics servlet to the web server.
    mWebServer.addHandler(mMetricsServlet.getHandler());
    // start web ui
    mWebServer.start();
  }

  private void registerServices(TMultiplexedProcessor processor, Map<String, TProcessor> services) {
    for (Map.Entry<String, TProcessor> service : services.entrySet()) {
      processor.registerProcessor(service.getKey(), service.getValue());
    }
  }

  protected void startServingRPCServer() {
    // set up multiplexed thrift processors
    TMultiplexedProcessor processor = new TMultiplexedProcessor();
    // register master services
    for (Master master : mRegistry.getMasters()) {
      registerServices(processor, master.getServices());
    }
    // register meta services
    processor.registerProcessor(Constants.META_MASTER_SERVICE_NAME,
        new MetaMasterClientService.Processor<>(
        new MetaMasterClientServiceHandler(this)));

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

  /**
   * Stops serving, trying stop RPC server and web ui server and letting {@link MetricsSystem} stop
   * all the sinks.
   *
   * @throws Exception if the underlying jetty server throws an exception
   */
  protected void stopServing() throws Exception {
    if (mMasterServiceServer != null) {
      mMasterServiceServer.stop();
      mMasterServiceServer = null;
    }
    if (mWebServer != null) {
      mWebServer.stop();
      mWebServer = null;
    }
    MetricsSystem.stopSinks();
    mIsServing = false;
  }

  private void connectToUFS() throws IOException {
    String ufsAddress = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsAddress);
    ufs.connectFromMaster(NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC));
  }
}
