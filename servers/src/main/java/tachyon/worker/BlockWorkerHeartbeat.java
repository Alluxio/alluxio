package tachyon.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterClient;
import tachyon.thrift.Command;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.util.ThreadFactoryUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Task that sends a block worker heartbeat to the master periodically and carries out the requested
 * commands returned by the master. Manages its own MasterClient instance.
 */
public class BlockWorkerHeartbeat implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final CoreWorker mCoreWorker;
  private final ExecutorService mMasterClientExecutorService;
  private final MasterClient mMasterClient;
  private boolean mRunning;
  private final int mHeartbeatIntervalMs;
  private final int mHeartbeatTimeoutMs;
  private final int mWorkerId;

  BlockWorkerHeartbeat(CoreWorker coreWorker, TachyonConf tachyonConf, int workerId) {
    mCoreWorker = coreWorker;
    mRunning = true;
    mMasterClientExecutorService =
        Executors.newFixedThreadPool(1, ThreadFactoryUtils.daemon("worker-client-heartbeat-%d"));
    mMasterClient =
        new MasterClient(getMasterAddress(tachyonConf), mMasterClientExecutorService, tachyonConf);
    mHeartbeatIntervalMs =
        tachyonConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS, Constants.SECOND_MS);
    mHeartbeatTimeoutMs =
        tachyonConf.getInt(Constants.WORKER_HEARTBEAT_TIMEOUT_MS, 10 * Constants.SECOND_MS);
    mWorkerId = workerId;
  }

  private InetSocketAddress getMasterAddress(TachyonConf tachyonConf) {
    String masterHostname =
        tachyonConf.get(Constants.MASTER_HOSTNAME, NetworkUtils.getLocalHostName(tachyonConf));
    int masterPort = tachyonConf.getInt(Constants.MASTER_PORT, Constants.DEFAULT_MASTER_PORT);
    return new InetSocketAddress(masterHostname, masterPort);
  }

  @Override
  public void run() {
    long lastHeartbeatMs = System.currentTimeMillis();
    Command cmd = null;
    while (mRunning) {
      long diff = System.currentTimeMillis() - lastHeartbeatMs;
      if (diff < mHeartbeatIntervalMs) {
        LOG.debug("Heartbeat process takes {} ms.", diff);
        CommonUtils.sleepMs(LOG, mHeartbeatIntervalMs - diff);
      } else {
        LOG.warn("Heartbeat took " + diff + " ms, expected " + mHeartbeatIntervalMs + " ms.");
      }

      try {
        BlockWorkerReport blockReport = mCoreWorker.getReport();
        cmd =
            mMasterClient.worker_heartbeat(mWorkerId, blockReport.getUsedBytesOnTiers(),
                blockReport.getRemovedBlocks(), blockReport.getAddedBlocks());
        lastHeartbeatMs = System.currentTimeMillis();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
        cmd = null;
        if (System.currentTimeMillis() - lastHeartbeatMs >= mHeartbeatTimeoutMs) {
          throw new RuntimeException("Heartbeat timeout "
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
            mCoreWorker.register();
            break;
          case Free:
            mCoreWorker.freeBlocks(cmd.mData);
            LOG.info("Free command: " + cmd);
            break;
          case Delete:
            LOG.info("Delete command: " + cmd);
            break;
          default:
            throw new RuntimeException("Un-recognized command from master " + cmd.toString());
        }
      }

      mCoreWorker.checkStatus();
    }
  }

  public void stop() {
    mRunning = false;
    mMasterClient.close();
    mMasterClientExecutorService.shutdown();
  }
}
