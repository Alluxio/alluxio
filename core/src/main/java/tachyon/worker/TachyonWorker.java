package tachyon.worker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.Users;
import tachyon.Version;
import tachyon.conf.CommonConf;
import tachyon.conf.WorkerConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.Command;
import tachyon.thrift.NetAddress;
import tachyon.thrift.WorkerService;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.worker.netty.NettyDataServer;
import tachyon.worker.nio.NIODataServer;

/**
 * Entry point for a worker daemon.
 */
public class TachyonWorker implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Create a new TachyonWorker
   * 
   * @param masterAddress The TachyonMaster's address
   * @param workerAddress This TachyonWorker's address
   * @param dataPort This TachyonWorker's data server's port
   * @param selectorThreads The number of selector threads of the worker's thrift server
   * @param acceptQueueSizePerThreads The accept queue size per thread of the worker's thrift server
   * @param workerThreads The number of threads of the worker's thrift server
   * @param localFolder This TachyonWorker's local folder's path
   * @param spaceLimitBytes The maximum memory space this TachyonWorker can use, in bytes
   * @return The new TachyonWorker
   */
  public static synchronized TachyonWorker createWorker(InetSocketAddress masterAddress,
      InetSocketAddress workerAddress, int dataPort, int selectorThreads,
      int acceptQueueSizePerThreads, int workerThreads, String localFolder, long spaceLimitBytes) {
    return new TachyonWorker(masterAddress, workerAddress, dataPort, selectorThreads,
        acceptQueueSizePerThreads, workerThreads, localFolder, spaceLimitBytes);
  }

  /**
   * Create a new TachyonWorker
   * 
   * @param masterAddress The TachyonMaster's address. e.g., localhost:19998
   * @param workerAddress This TachyonWorker's address. e.g., localhost:29998
   * @param dataPort This TachyonWorker's data server's port
   * @param selectorThreads The number of selector threads of the worker's thrift server
   * @param acceptQueueSizePerThreads The accept queue size per thread of the worker's thrift server
   * @param workerThreads The number of threads of the worker's thrift server
   * @param localFolder This TachyonWorker's local folder's path
   * @param spaceLimitBytes The maximum memory space this TachyonWorker can use, in bytes
   * @return The new TachyonWorker
   */
  public static synchronized TachyonWorker createWorker(String masterAddress, String workerAddress,
      int dataPort, int selectorThreads, int acceptQueueSizePerThreads, int workerThreads,
      String localFolder, long spaceLimitBytes) {
    String[] address = masterAddress.split(":");
    InetSocketAddress master = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
    address = workerAddress.split(":");
    InetSocketAddress worker = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
    return new TachyonWorker(master, worker, dataPort, selectorThreads, acceptQueueSizePerThreads,
        workerThreads, localFolder, spaceLimitBytes);
  }

  private static String getMasterLocation(String[] args) {
    WorkerConf wConf = WorkerConf.get();
    String confFileMasterLoc = wConf.MASTER_HOSTNAME + ":" + wConf.MASTER_PORT;
    String masterLocation;
    if (args.length < 1) {
      masterLocation = confFileMasterLoc;
    } else {
      masterLocation = args[0];
      if (masterLocation.indexOf(":") == -1) {
        masterLocation += ":" + wConf.MASTER_PORT;
      }
      if (!masterLocation.equals(confFileMasterLoc)) {
        LOG.warn("Master Address in configuration file(" + confFileMasterLoc + ") is different "
            + "from the command line one(" + masterLocation + ").");
      }
    }
    return masterLocation;
  }

  public static void main(String[] args) throws UnknownHostException {
    if (args.length > 1) {
      LOG.info("Usage: java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
          + "tachyon.Worker [<MasterHost:Port>]");
      System.exit(-1);
    }

    WorkerConf wConf = WorkerConf.get();

    String resolvedWorkerHost = NetworkUtils.getLocalHostName();
    LOG.info("Resolved local TachyonWorker host to " + resolvedWorkerHost);

    TachyonWorker worker =
        TachyonWorker.createWorker(getMasterLocation(args), resolvedWorkerHost + ":" + wConf.PORT,
            wConf.DATA_PORT, wConf.SELECTOR_THREADS, wConf.QUEUE_SIZE_PER_SELECTOR,
            wConf.SERVER_THREADS, wConf.DATA_FOLDER, wConf.MEMORY_SIZE);
    try {
      worker.start();
    } catch (Exception e) {
      LOG.error("Uncaught exception terminating worker", e);
      throw new RuntimeException(e);
    }
  }

  private final InetSocketAddress mMasterAddress;
  private final NetAddress mWorkerAddress;
  private TServer mServer;

  private TNonblockingServerSocket mServerTNonblockingServerSocket;
  private WorkerStorage mWorkerStorage;

  private WorkerServiceHandler mWorkerServiceHandler;

  private final DataServer mDataServer;

  private Thread mHeartbeatThread;

  private volatile boolean mStop = false;

  private final int mPort;
  private final int mDataPort;

  /**
   * @param masterAddress The TachyonMaster's address.
   * @param workerAddress This TachyonWorker's address.
   * @param dataPort This TachyonWorker's data server's port
   * @param selectorThreads The number of selector threads of the worker's thrift server
   * @param acceptQueueSizePerThreads The accept queue size per thread of the worker's thrift server
   * @param workerThreads The number of threads of the worker's thrift server
   * @param dataFolder This TachyonWorker's local folder's path
   * @param memoryCapacityBytes The maximum memory space this TachyonWorker can use, in bytes
   */
  private TachyonWorker(InetSocketAddress masterAddress, InetSocketAddress workerAddress,
      int dataPort, int selectorThreads, int acceptQueueSizePerThreads, int workerThreads,
      String dataFolder, long memoryCapacityBytes) {
    CommonConf.assertValidPort(masterAddress);
    CommonConf.assertValidPort(workerAddress);
    CommonConf.assertValidPort(dataPort);

    mMasterAddress = masterAddress;

    mWorkerStorage = new WorkerStorage(mMasterAddress, dataFolder, memoryCapacityBytes);

    mWorkerServiceHandler = new WorkerServiceHandler(mWorkerStorage);

    // Extract the port from the generated socket.
    // When running tests, its great to use port '0' so the system will figure out what port to use
    // (any random free port).
    // In a production or any real deployment setup, port '0' should not be used as it will make
    // deployment more complicated.
    InetSocketAddress dataAddress = new InetSocketAddress(workerAddress.getHostName(), dataPort);
    BlocksLocker blockLocker = new BlocksLocker(mWorkerStorage, Users.sDATASERVER_USER_ID);
    mDataServer = createDataServer(dataAddress, blockLocker);
    mDataPort = mDataServer.getPort();

    mHeartbeatThread = new Thread(this);
    try {
      LOG.info("Tachyon Worker version " + Version.VERSION + " tries to start @ " + workerAddress);
      WorkerService.Processor<WorkerServiceHandler> processor =
          new WorkerService.Processor<WorkerServiceHandler>(mWorkerServiceHandler);

      mServerTNonblockingServerSocket = new TNonblockingServerSocket(workerAddress);
      mPort = NetworkUtils.getPort(mServerTNonblockingServerSocket);
      mServer =
          new TThreadedSelectorServer(new TThreadedSelectorServer.Args(
              mServerTNonblockingServerSocket).processor(processor)
              .selectorThreads(selectorThreads).acceptQueueSizePerThread(acceptQueueSizePerThreads)
              .workerThreads(workerThreads));
    } catch (TTransportException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
    mWorkerAddress =
        new NetAddress(workerAddress.getAddress().getCanonicalHostName(), mPort, mDataPort);
    mWorkerStorage.initialize(mWorkerAddress);
  }

  private DataServer createDataServer(final InetSocketAddress dataAddress,
      final BlocksLocker blockLocker) {
    switch (WorkerConf.get().NETWORK_TYPE) {
      case NIO:
        return new NIODataServer(dataAddress, blockLocker);
      case NETTY:
        return new NettyDataServer(dataAddress, blockLocker);
      default:
        throw new AssertionError("Unknown network type: " + WorkerConf.get().NETWORK_TYPE);
    }
  }

  /**
   * Gets the data port of the worker. For unit tests only.
   */
  public int getDataPort() {
    return mDataPort;
  }

  /**
   * Gets the metadata port of the worker. For unit tests only.
   */
  public int getMetaPort() {
    return mPort;
  }

  /**
   * Get the worker server handler class. This is for unit test only.
   * 
   * @return the WorkerServiceHandler
   */
  WorkerServiceHandler getWorkerServiceHandler() {
    return mWorkerServiceHandler;
  }

  @Override
  public void run() {
    long lastHeartbeatMs = System.currentTimeMillis();
    Command cmd = null;
    while (!mStop) {
      long diff = System.currentTimeMillis() - lastHeartbeatMs;
      if (diff < WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS) {
        LOG.debug("Heartbeat process takes {} ms.", diff);
        CommonUtils.sleepMs(LOG, WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS - diff);
      } else {
        LOG.error("Heartbeat process takes " + diff + " ms.");
      }

      try {
        cmd = mWorkerStorage.heartbeat();

        lastHeartbeatMs = System.currentTimeMillis();
      } catch (BlockInfoException e) {
        LOG.error(e.getMessage(), e);
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        try {
          mWorkerStorage.resetMasterClient();
        } catch (IOException e2) {
          LOG.error("Received exception while attempting to reset client", e2);
        }
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
        cmd = null;
        if (System.currentTimeMillis() - lastHeartbeatMs >= WorkerConf.get().HEARTBEAT_TIMEOUT_MS) {
          throw new RuntimeException("Timebeat timeout "
              + (System.currentTimeMillis() - lastHeartbeatMs) + "ms");
        }
      }

      if (cmd != null) {
        switch (cmd.mCommandType) {
          case Unknown:
            LOG.error("Unknown command: " + cmd);
            break;
          case Nothing:
            LOG.debug("Nothing command: {}", cmd);
            break;
          case Register:
            LOG.info("Register command: " + cmd);
            mWorkerStorage.register();
            break;
          case Free:
            mWorkerStorage.freeBlocks(cmd.mData);
            LOG.info("Free command: " + cmd);
            break;
          case Delete:
            LOG.info("Delete command: " + cmd);
            break;
          default:
            throw new RuntimeException("Un-recognized command from master " + cmd.toString());
        }
      }

      mWorkerStorage.checkStatus();
    }
  }

  /**
   * Start the data server thread and heartbeat thread of this TachyonWorker.
   */
  public void start() {
    mHeartbeatThread.start();

    LOG.info("The worker server started @ " + mWorkerAddress);
    mServer.serve();
    LOG.info("The worker server ends @ " + mWorkerAddress);
  }

  /**
   * Stop this TachyonWorker. Stop all the threads belong to this TachyonWorker.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public void stop() throws IOException, InterruptedException {
    mStop = true;
    mWorkerStorage.stop();
    mDataServer.close();
    mServer.stop();
    mServerTNonblockingServerSocket.close();
    while (!mDataServer.isClosed() || mServer.isServing() || mHeartbeatThread.isAlive()) {
      // TODO The reason to stop and close again is due to some issues in Thrift.
      mServer.stop();
      mServerTNonblockingServerSocket.close();
      CommonUtils.sleepMs(null, 100);
    }
    mHeartbeatThread.join();
  }
}
