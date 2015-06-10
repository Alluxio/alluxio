package tachyon.worker.block;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterClient;
import tachyon.thrift.Command;
import tachyon.thrift.NetAddress;
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
 *
 * When running, this task first requests a block report from the core worker, then sends it to
 * the master. The master may respond to the heartbeat with a command which will be executed.
 * After which, the task will wait for the elapsed time since its last heartbeat has reached the
 * heartbeat interval. Then the cycle will continue.
 *
 * If the task fails to heartbeat to the worker, it will destroy its old master client and
 * recreate it before retrying.
 */
// TODO: Find a better name for this
public class BlockMasterSync implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final BlockDataManager mBlockDataManager;
  private final ExecutorService mMasterClientExecutorService;
  private final NetAddress mWorkerAddress;
  private final TachyonConf mTachyonConf;
  private final int mHeartbeatIntervalMs;
  private final int mHeartbeatTimeoutMs;

  private MasterClient mMasterClient;
  private boolean mRunning;
  private int mWorkerId;

  BlockMasterSync(BlockDataManager blockDataManager, TachyonConf tachyonConf, NetAddress workerAddress) {
    mBlockDataManager = blockDataManager;
    mWorkerAddress = workerAddress;
    mTachyonConf = tachyonConf;
    mMasterClientExecutorService =
        Executors.newFixedThreadPool(1, ThreadFactoryUtils.daemon("worker-client-heartbeat-%d"));
    mMasterClient =
        new MasterClient(getMasterAddress(), mMasterClientExecutorService, mTachyonConf);
    mHeartbeatIntervalMs =
        mTachyonConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS, Constants.SECOND_MS);
    mHeartbeatTimeoutMs =
        mTachyonConf.getInt(Constants.WORKER_HEARTBEAT_TIMEOUT_MS, 10 * Constants.SECOND_MS);

    mRunning = true;
    mWorkerId = 0;
  }

  private InetSocketAddress getMasterAddress() {
    String masterHostname =
        mTachyonConf.get(Constants.MASTER_HOSTNAME, NetworkUtils.getLocalHostName(mTachyonConf));
    int masterPort = mTachyonConf.getInt(Constants.MASTER_PORT, Constants.DEFAULT_MASTER_PORT);
    return new InetSocketAddress(masterHostname, masterPort);
  }

  private void handleMasterCommand(Command cmd) {
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
          LOG.info("Free command: " + cmd);
          for (long block : cmd.mData) {
            try {
              // TODO: Define constants for system user.
              mBlockDataManager.freeBlock(-1, block);
            } catch (IOException ioe) {
              LOG.error("Failed to free blocks: " + cmd.mData, ioe);
            }
          }
          break;
        case Delete:
          LOG.info("Delete command: " + cmd);
          break;
        default:
          throw new RuntimeException("Un-recognized command from master " + cmd.toString());
      }
    }
  }

  public void registerWithMaster() {
    BlockHeartbeatReport blockReport = mBlockDataManager.getReport();
    StoreMeta storeMeta = mBlockDataManager.getStoreMeta();
    // TODO: Are retries necessary?
    int assignedId =
        mMasterClient.worker_register(mWorkerAddress, storeMeta.getCapacityBytesOnTiers(),
            blockReport.getUsedBytesOnTiers(), storeMeta.getBlockList());
    mWorkerId = assignedId;
  }

  private void resetMasterClient() {
    mMasterClient.close();
    mMasterClient =
        new MasterClient(getMasterAddress(), mMasterClientExecutorService, mTachyonConf);
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
        BlockHeartbeatReport blockReport = mBlockDataManager.getReport();
        cmd = mMasterClient.worker_heartbeat(mWorkerId, blockReport.getUsedBytesOnTiers(),
            blockReport.getRemovedBlocks(), blockReport.getAddedBlocks());
        lastHeartbeatMs = System.currentTimeMillis();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        resetMasterClient();
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
        cmd = null;
        diff = System.currentTimeMillis() - lastHeartbeatMs;
        if (diff >= mHeartbeatTimeoutMs) {
          throw new RuntimeException("Heartbeat timeout " + diff + "ms");
        }
      }
      // TODO: Is there a way to make this async? Could take much longer than heartbeat timeout.
      handleMasterCommand(cmd);
      // TODO: This should go in its own thread
      mBlockDataManager.cleanupUsers();
    }
  }

  public void stop() {
    mRunning = false;
    mMasterClient.close();
    mMasterClientExecutorService.shutdown();
  }
}
