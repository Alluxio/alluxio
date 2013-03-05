package tachyon;

import java.net.InetSocketAddress;

import org.apache.thrift.TException;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.thrift.Command;
import tachyon.thrift.WorkerService;

/**
 * Entry point for a worker daemon. Worker class is singleton.
 * 
 * @author haoyuan
 */
public class Worker implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

  private static Worker WORKER = null;

  private final InetSocketAddress MasterAddress;
  private final InetSocketAddress WorkerAddress;
  private final String DataFolder;
  private final long MemoryCapacityBytes;

  private TServer mServer;
  private WorkerServiceHandler mWorkerServiceHandler;
  private DataServer mDataServer;

  private Worker(InetSocketAddress masterAddress, InetSocketAddress workerAddress, 
      int selectorThreads, int acceptQueueSizePerThreads, int workerThreads,
      String dataFolder, long memoryCapacityBytes) {
    DataFolder = dataFolder;
    MemoryCapacityBytes = memoryCapacityBytes;

    MasterAddress = masterAddress;
    WorkerAddress = workerAddress;

    mWorkerServiceHandler = new WorkerServiceHandler(
        MasterAddress, WorkerAddress, DataFolder, MemoryCapacityBytes);

    mDataServer = new DataServer(new InetSocketAddress(
        workerAddress.getHostName(), Config.WORKER_DATA_SERVER_PORT),
        mWorkerServiceHandler);
    new Thread(mDataServer).start();

    new Thread(this).start();

    try {
      LOG.info("The worker server tries to start @ " + workerAddress);
      WorkerService.Processor<WorkerServiceHandler> processor =
          new WorkerService.Processor<WorkerServiceHandler>(mWorkerServiceHandler);

      // TODO This is for Thrift 0.8 or newer.
      //      mServer = new TThreadedSelectorServer(new TThreadedSelectorServer
      //          .Args(new TNonblockingServerSocket(workerAddress)).processor(processor)
      //          .selectorThreads(selectorThreads).acceptQueueSizePerThread(acceptQueueSizePerThreads)
      //          .workerThreads(workerThreads));

      // This is for Thrift 0.7.0, for Hive compatibility. 
      mServer = new THsHaServer(new THsHaServer.Args(new TNonblockingServerSocket(workerAddress)).
          processor(processor).workerThreads(workerThreads));

      LOG.info("The worker server started @ " + workerAddress);
      mServer.serve();
      LOG.info("The worker server ends @ " + workerAddress);
    } catch (TTransportException e) {
      LOG.error(e.getMessage(), e);
      System.exit(-1);
    }
  }

  @Override
  public void run() {
    long lastHeartbeatMs = System.currentTimeMillis();
    Command cmd = null;
    while (true) {
      long diff = System.currentTimeMillis() - lastHeartbeatMs;
      if (diff < Config.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS) {
        LOG.warn("Last heartbeat related process takes " + diff + " ms.");
        CommonUtils.sleep(Config.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS - diff);
      } else {
        LOG.warn("Last heartbeat related process takes " + diff + " ms.");
      }

      try {
        cmd = mWorkerServiceHandler.heartbeat();

        lastHeartbeatMs = System.currentTimeMillis();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mWorkerServiceHandler.resetMasterClient();
        CommonUtils.sleep(1000);
        cmd = null;
        if (System.currentTimeMillis() - lastHeartbeatMs >= Config.WORKER_HEARTBEAT_TIMEOUT_MS) {
          System.exit(-1);
        }
      }

      if (cmd != null) {
        switch (cmd.mCommandType) {
          case Unknown :
            LOG.error("Heartbeat got Unknown command: " + cmd);
            break;
          case Nothing :
            LOG.debug("Heartbeat got Nothing command: " + cmd);
            break;
          case Register :
            mWorkerServiceHandler.register();
            LOG.info("Heartbeat got Register command: " + cmd);
            break;
          case Free :
            LOG.info("Heartbeat got Free command: " + cmd);
            break;
          case Delete :
            LOG.info("Heartbeat got Delete command: " + cmd);
            break;
          default :
            CommonUtils.runtimeException("Got un-recognized command from master " + cmd.toString());
        }
      }

      mWorkerServiceHandler.checkStatus();
    }
  }

  public static synchronized Worker createWorker(InetSocketAddress masterAddress, 
      InetSocketAddress workerAddress, int selectorThreads, int acceptQueueSizePerThreads,
      int workerThreads, String localFolder, long spaceLimitBytes) {
    if (WORKER == null) {
      WORKER = new Worker(masterAddress, workerAddress, selectorThreads, 
          acceptQueueSizePerThreads, workerThreads, localFolder, spaceLimitBytes);
    }
    return WORKER;
  }

  public static void main(String[] args) {
    if (args.length == 0) {
      Worker.createWorker(
          new InetSocketAddress(Config.MASTER_HOSTNAME, Config.MASTER_PORT),
          new InetSocketAddress("localhost", Config.WORKER_PORT),
          Config.WORKER_SELECTOR_THREADS, Config.WORKER_QUEUE_SIZE_PER_SELECTOR,
          Config.WORKER_WORKER_THREADS, Config.WORKER_DATA_FOLDER,
          Config.WORKER_MEMORY_SIZE);
    } else if (args.length == 1) {
      Worker.createWorker(
          new InetSocketAddress(Config.MASTER_HOSTNAME, Config.MASTER_PORT),
          new InetSocketAddress(args[0], Config.WORKER_PORT),
          Config.WORKER_SELECTOR_THREADS, Config.WORKER_QUEUE_SIZE_PER_SELECTOR,
          Config.WORKER_WORKER_THREADS, Config.WORKER_DATA_FOLDER,
          Config.WORKER_MEMORY_SIZE);
    } else {
      LOG.info("java -cp target/tachyon-1.0-SNAPSHOT-jar-with-dependencies.jar tachyon.Worker " +
          "<WorkerHostName>");
    }
  }
}