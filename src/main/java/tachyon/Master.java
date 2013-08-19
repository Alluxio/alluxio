package tachyon;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.log4j.Logger;

import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.thrift.CoordinatorService;
import tachyon.thrift.MasterService;
import tachyon.web.UIWebServer;

/**
 * Entry point for the Master program.
 */
public class Master {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private MasterInfo mMasterInfo;
  private InetSocketAddress mMasterAddress;
  private UIWebServer mWebServer;
  private TServer mMasterServiceServer;
  private MasterServiceHandler mMasterServiceHandler;
  private Thread mMasterThread;
  private Journal mJournal;
  private int mWebPort;
  private int mWorkerThreads;

  private boolean mZookeeperMode = false;
  private boolean mIsLeader = false;
  private LeaderSelectorClient mLeaderSelectorClient = null;
  private InetSocketAddress mMasterCoordinatorAddress;
  private TServer mCoordinatorServiceServer = null;
  private CoordinatorServiceHandler mCoordinatorServiceHandler = null;
  private Thread mCoordinatorServiceThread = null;

  public Master(InetSocketAddress address, int webPort, int selectorThreads, 
      int acceptQueueSizePerThreads, int workerThreads, String imageFile, String editLogFile) {
    if (CommonConf.get().USE_ZOOKEEPER) {
      mZookeeperMode = true;
    }

    mWebPort = webPort;
    mWorkerThreads = workerThreads;

    try {
      mMasterAddress = address;

      mJournal = new Journal(MasterConf.get().JOURNAL_FOLDER, imageFile, editLogFile);

      if (mZookeeperMode) {
        CommonConf conf = CommonConf.get();
        mLeaderSelectorClient = new LeaderSelectorClient(conf.ZOOKEEPER_ADDRESS,
            conf.ZOOKEEPER_ELECTION_PATH, conf.ZOOKEEPER_LEADER_PATH, address.toString());
        mCoordinatorServiceHandler = new CoordinatorServiceHandler(mJournal);
        CoordinatorService.Processor<CoordinatorServiceHandler> coordinatorServiceProcessor = 
            new CoordinatorService.Processor<CoordinatorServiceHandler>(mCoordinatorServiceHandler);
        mMasterCoordinatorAddress =
            new InetSocketAddress(mMasterAddress.getHostName(), mMasterAddress.getPort() + 2);
        mCoordinatorServiceServer = new THsHaServer(new THsHaServer.Args(
            new TNonblockingServerSocket(mMasterCoordinatorAddress)).
            processor(coordinatorServiceProcessor).workerThreads(1));
      }
    } catch (TTransportException e) {
      LOG.error(e.getMessage(), e);
      System.exit(-1);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      System.exit(-1);
    }
  }

  private void setup() throws IOException, TTransportException {
    mMasterInfo = new MasterInfo(mMasterAddress, mJournal);

    mWebServer = new UIWebServer("Tachyon Master Server",
        new InetSocketAddress(mMasterAddress.getHostName(), mWebPort), mMasterInfo);

    mMasterServiceHandler = new MasterServiceHandler(mMasterInfo);
    MasterService.Processor<MasterServiceHandler> masterServiceProcessor = 
        new MasterService.Processor<MasterServiceHandler>(mMasterServiceHandler);

    // TODO This is for Thrift 0.8 or newer.
    //      mServer = new TThreadedSelectorServer(new TThreadedSelectorServer
    //          .Args(new TNonblockingServerSocket(address)).processor(processor)
    //          .selectorThreads(selectorThreads).acceptQueueSizePerThread(acceptQueueSizePerThreads)
    //          .workerThreads(workerThreads));

    // This is for Thrift 0.7.0, for Hive compatibility. 
    mMasterServiceServer = new THsHaServer(new THsHaServer.Args(new TNonblockingServerSocket(
        mMasterAddress)).processor(masterServiceProcessor).workerThreads(mWorkerThreads));
  }

  public void start() {
    if (mZookeeperMode) {
      Runnable runCoordinatorServiceServer = new Runnable() {
        public void run() {
          mCoordinatorServiceServer.serve();
        }
      };
      mCoordinatorServiceThread = new Thread(runCoordinatorServiceServer);
      mCoordinatorServiceThread.start();

      Runnable runMaster = new Runnable() {
        public void run() {
          mMasterServiceServer.serve();
        }
      };

      try {
        mLeaderSelectorClient.start();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        System.exit(-1);
      }

      boolean running = false;
      while (true) {
        if (mLeaderSelectorClient.isLeader()) {
          if (!running) {
            mMasterThread = new Thread(runMaster);
            mMasterThread.start();
            running = true;
          }
        } else {
          if (running) {
            mMasterServiceServer.stop();
            running = false;
          }
        }

        CommonUtils.sleepMs(LOG, 100);
      }
    } else {
      try {
        setup();
      } catch (TTransportException | IOException e) {
        LOG.error(e.getMessage(), e);
        System.exit(-1);
      }

      mWebServer.startWebServer();
      LOG.info("The master server started @ " + mMasterAddress);
      mMasterServiceServer.serve();
      LOG.info("The master server ended @ " + mMasterAddress);
    }
  }

  public void stop() throws Exception {
    mWebServer.shutdownWebServer();
    mMasterInfo.stop();
    mMasterServiceServer.stop();
  }

  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar " +
          "tachyon.Master");
      System.exit(-1);
    }
    MasterConf mConf = MasterConf.get();
    Master master = new Master(new InetSocketAddress(mConf.HOSTNAME, mConf.PORT),
        mConf.WEB_PORT, mConf.SELECTOR_THREADS, mConf.QUEUE_SIZE_PER_SELECTOR, 
        mConf.SERVER_THREADS, "image.data", "log.data");
    master.start();
  }

  /**
   * Get MasterInfo instance for Unit Test
   * @return MasterInfo of the Master  
   */
  MasterInfo getMasterInfo() {
    return mMasterInfo;
  }

  /**
   * Get whether the system is for zookeeper mode, for unit test only.
   * @return true if the master is under zookeeper mode, false otherwise.
   */
  boolean isZookeeperMode() {
    return mZookeeperMode;
  }

  /**
   * Get wehether the system is the leader under zookeeper mode, for unit test only.
   * @return true if the system is the leader under zookeeper mode, false otherwise.
   */
  boolean isLeader() {
    return mIsLeader;
  }
}