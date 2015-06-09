package tachyon.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterClient;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.Command;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.util.ThreadFactoryUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Task that carries out the necessary block worker to master communications, including register
 * and heartbeat. This class manages its own {@link tachyon.master.MasterClient}.
 */
// TODO: Find a better name for this
public class BlockWorkerHeartbeat implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final CoreWorker mCoreWorker;
  private final ExecutorService mMasterClientExecutorService;
  private final InetSocketAddress mWorkerAddress;
  private final MasterClient mMasterClient;
  private final int mHeartbeatIntervalMs;
  private final int mHeartbeatTimeoutMs;

  private boolean mRunning;
  private int mWorkerId;

  BlockWorkerHeartbeat(CoreWorker coreWorker, TachyonConf tachyonConf, InetSocketAddress
      workerAddress) {
    mCoreWorker = coreWorker;
    mWorkerAddress = workerAddress;
    mMasterClientExecutorService =
        Executors.newFixedThreadPool(1, ThreadFactoryUtils.daemon("worker-client-heartbeat-%d"));
    mMasterClient =
        new MasterClient(getMasterAddress(tachyonConf), mMasterClientExecutorService, tachyonConf);
    mHeartbeatIntervalMs =
        tachyonConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS, Constants.SECOND_MS);
    mHeartbeatTimeoutMs =
        tachyonConf.getInt(Constants.WORKER_HEARTBEAT_TIMEOUT_MS, 10 * Constants.SECOND_MS);

    mRunning = true;
    mWorkerId = 0;
  }

  private InetSocketAddress getMasterAddress(TachyonConf tachyonConf) {
    String masterHostname =
        tachyonConf.get(Constants.MASTER_HOSTNAME, NetworkUtils.getLocalHostName(tachyonConf));
    int masterPort = tachyonConf.getInt(Constants.MASTER_PORT, Constants.DEFAULT_MASTER_PORT);
    return new InetSocketAddress(masterHostname, masterPort);
  }

  private void registerWithMaster() {
    int assignedId = 0;
    BlockWorkerReport blockReport = mCoreWorker.getReport();
    StoreMeta storeMeta = mCoreWorker.getStoreMeta();
    while (assignedId == 0) {
      try {
        assignedId =
            mMasterClient.worker_register(mWorkerAddress, storeMeta.getCapacityBytesOnTiers(),
                blockReport.getUsedBytesOnTiers(), storeMeta.getBlockList());
      } catch (BlockInfoException e) {
        LOG.error(e.getMessage(), e);
        assignedId = 0;
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        assignedId = 0;
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
      }
    }
    if (mWorkerId != 0 && mWorkerId != assignedId) {
      LOG.warn("Received new worker id from master: " + assignedId + ". Old id: " + mWorkerId);
    }
    mWorkerId = assignedId;
  }

  public int getWorkerId() {
    return mWorkerId;
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
            registerWithMaster();
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
