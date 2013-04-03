package tachyon;

import java.net.InetSocketAddress;

import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.log4j.Logger;

import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.thrift.MasterService;
import tachyon.web.UIWebServer;

/**
 * Entry point for the Master program. Master class is singleton.
 */
public class Master {
  private static final Logger LOG = Logger.getLogger(CommonConf.get().LOGGER_TYPE);

  private static Master MASTER = null;

  private MasterInfo mMasterInfo;
  private UIWebServer mWebServer;
  private TServer mServer;
  private MasterServiceHandler mMasterServiceHandler;

  private Master(InetSocketAddress address, int selectorThreads, int acceptQueueSizePerThreads,
      int workerThreads) {
    try {
      mMasterInfo = new MasterInfo(address);

      mWebServer = new UIWebServer("Tachyon Master Server",
          new InetSocketAddress(address.getHostName(), address.getPort() + 1), mMasterInfo);
      mWebServer.startWebServer();

      mMasterServiceHandler = new MasterServiceHandler(mMasterInfo);
      MasterService.Processor<MasterServiceHandler> processor = 
          new MasterService.Processor<MasterServiceHandler>(mMasterServiceHandler);

      // TODO This is for Thrift 0.8 or newer.
      //      mServer = new TThreadedSelectorServer(new TThreadedSelectorServer
      //          .Args(new TNonblockingServerSocket(address)).processor(processor)
      //          .selectorThreads(selectorThreads).acceptQueueSizePerThread(acceptQueueSizePerThreads)
      //          .workerThreads(workerThreads));

      // This is for Thrift 0.7.0, for Hive compatibility. 
      mServer = new THsHaServer(new THsHaServer.Args(new TNonblockingServerSocket(address)).
          processor(processor).workerThreads(workerThreads));

      LOG.info("The master server started @ " + address);
      mServer.serve();
      LOG.info("The master server ended @ " + address);
    } catch (TTransportException e) {
      LOG.error(e.getMessage(), e);
      System.exit(-1);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      System.exit(-1);
    }
  }

  public static synchronized Master createMaster(InetSocketAddress address, int selectorThreads,
      int acceptQueueSizePerThreads, int workerThreads) {
    if (MASTER == null) {
      MASTER = new Master(address, selectorThreads, acceptQueueSizePerThreads, workerThreads);
    }
    return MASTER;
  }

  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar " +
          "tachyon.Master");
      System.exit(-1);
    }
    MasterConf mConf = MasterConf.get();
    Master.createMaster(new InetSocketAddress(mConf.HOSTNAME, mConf.PORT),
        mConf.SELECTOR_THREADS, mConf.QUEUE_SIZE_PER_SELECTOR, mConf.SERVER_THREADS);
  }
}