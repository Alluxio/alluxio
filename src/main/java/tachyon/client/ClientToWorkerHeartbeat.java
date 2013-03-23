package tachyon.client;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.CommonUtils;
import tachyon.Config;
import tachyon.WorkerClient;

class ClientToWorkerHeartbeat implements Runnable {
  private final Logger LOG = Logger.getLogger(Config.LOGGER_TYPE);
  private final WorkerClient WORKER_CLIENT;
  private final long USER_ID;

  public ClientToWorkerHeartbeat(WorkerClient workerClient, long userId) {
    WORKER_CLIENT = workerClient;
    USER_ID = userId;
  }

  @Override
  public void run() {
    while (true) {
      try {
        WORKER_CLIENT.userHeartbeat(USER_ID);
      } catch (TException e) {
        LOG.error(e.getMessage());
        break;
      }

      CommonUtils.sleepMs(LOG, Config.USER_HEARTBEAT_INTERVAL_MS);
    }
  }
}